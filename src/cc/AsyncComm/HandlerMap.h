/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_HANDLERMAP_H
#define HYPERTABLE_HANDLERMAP_H

#include <cassert>

//#define HT_DISABLE_LOG_DEBUG

#include <boost/thread/condition.hpp>

#include "Common/Mutex.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/ReferenceCount.h"
#include "Common/SockAddrMap.h"
#include "Common/Time.h"
#include "Common/Timer.h"

#include "CommAddress.h"
#include "CommBuf.h"
#include "IOHandlerData.h"
#include "IOHandlerDatagram.h"
#include "ProxyMap.h"

namespace Hypertable {

  class HandlerMap : public ReferenceCount {

  public:

  HandlerMap() : m_proxies_loaded(false) { }

    void insert_handler(IOHandler *handler) {
      ScopedLock lock(m_mutex);
      assert(m_handler_map.find(handler->get_address()) == m_handler_map.end());
      m_handler_map[handler->get_address()] = handler;
    }

    void insert_handler(IOHandlerData *handler) {
      ScopedLock lock(m_mutex);
      assert(m_handler_map.find(handler->get_address()) == m_handler_map.end());
      m_handler_map[handler->get_address()] = handler;
      if (ReactorFactory::proxy_master) {
	CommBufPtr comm_buf = m_proxy_map.create_update_message();
	comm_buf->write_header_and_reset();
	int error = handler->send_message(comm_buf);
	HT_ASSERT(error == Error::OK);
      }
    }

    int set_alias(const InetAddr &addr, const InetAddr &alias) {
      ScopedLock lock(m_mutex);
      SockAddrMap<IOHandlerPtr>::iterator iter;

      if (m_handler_map.find(alias) != m_handler_map.end())
        return Error::COMM_CONFLICTING_ADDRESS;

      if ((iter = m_handler_map.find(addr)) == m_handler_map.end())
        return Error::COMM_NOT_CONNECTED;

      (*iter).second->set_alias(alias);
      m_handler_map[alias] = (*iter).second;

      return Error::OK;
    }

    int add_proxy(const String &proxy, const InetAddr &addr) {
      ScopedLock lock(m_mutex);
      ProxyMapT new_map, invalidated_map;

      m_proxy_map.update_mapping(proxy, addr, invalidated_map, new_map);

      foreach(const ProxyMapT::value_type &v, invalidated_map) {
	IOHandler *handler = lookup_handler(v.second);
	if (handler)
	  handler->set_proxy("");
      }

      foreach(const ProxyMapT::value_type &v, new_map) {
	IOHandler *handler = lookup_handler(v.second);
	if (handler)
	  handler->set_proxy(v.first);
      }

      return propagate_mappings(new_map);
    }

    /**
     * Returns the proxy map
     *
     * @param proxy_map reference to proxy map to be filled in
     */
    void get_proxy_map(ProxyMapT &proxy_map) {
      m_proxy_map.get_map(proxy_map);
    }


    void update_proxies(const char *message, size_t message_len) {
      ScopedLock lock(m_mutex);
      String mappings(message, message_len);
      ProxyMapT new_map, invalidated_map;

      HT_ASSERT(!ReactorFactory::proxy_master);

      m_proxy_map.update_mappings(mappings, invalidated_map, new_map);

      foreach(const ProxyMapT::value_type &v, invalidated_map) {
	IOHandler *handler = lookup_handler(v.second);
	if (handler)
	  handler->set_proxy("");
      }

      foreach(const ProxyMapT::value_type &v, new_map) {
	IOHandler *handler = lookup_handler(v.second);
	if (handler)
	  handler->set_proxy(v.first);
      }

      m_proxies_loaded = true;
      m_cond.notify_all();
    }

    bool wait_for_proxy_load(Timer &timer) {
      ScopedLock lock(m_mutex);
      boost::xtime drop_time;

      timer.start();

      while (!m_proxies_loaded) {
        boost::xtime_get(&drop_time, boost::TIME_UTC);
        xtime_add_millis(drop_time, timer.remaining());
        if (!m_cond.timed_wait(lock, drop_time))
          return false;
      }
      return true;
    }

    int contains_data_handler(const CommAddress &addr) {
      IOHandlerDataPtr data_handler;
      return lookup_data_handler(addr, data_handler);
    }

    int lookup_data_handler(const CommAddress &addr,
			    IOHandlerDataPtr &io_handler_data) {
      ScopedLock lock(m_mutex);
      InetAddr inet_addr;
      int error;

      if ((error = translate_address(addr, &inet_addr)) != Error::OK)
	return error;

      IOHandler *handler = lookup_handler(inet_addr);
      if (handler) {
        io_handler_data = dynamic_cast<IOHandlerData *>(handler);
        if (io_handler_data)
          return Error::OK;
      }
      return Error::COMM_NOT_CONNECTED;
    }

    void insert_datagram_handler(IOHandler *handler) {
      ScopedLock lock(m_mutex);
      HT_ASSERT(m_datagram_handler_map.find(handler->get_local_address())
                == m_datagram_handler_map.end());
      m_datagram_handler_map[handler->get_local_address()] = handler;
    }

    int lookup_datagram_handler(const CommAddress &addr,
				IOHandlerDatagramPtr &io_handler_dg) {
      ScopedLock lock(m_mutex);
      InetAddr inet_addr;
      int error;

      if ((error = translate_address(addr, &inet_addr)) != Error::OK)
	return error;

      SockAddrMap<IOHandlerPtr>::iterator iter =
	m_datagram_handler_map.find(inet_addr);

      if (iter == m_handler_map.end())
	return Error::COMM_NOT_CONNECTED;

      io_handler_dg = (IOHandlerDatagram *)(*iter).second.get();

      return Error::OK;
    }

    int remove_handler(const CommAddress &addr, IOHandlerPtr &handler) {
      SockAddrMap<IOHandlerPtr>::iterator iter;
      InetAddr inet_addr;
      int error;

      if ((error = translate_address(addr, &inet_addr)) != Error::OK)
	return error;

      if ((iter = m_handler_map.find(inet_addr)) != m_handler_map.end()) {
        handler = (*iter).second;
        m_handler_map.erase(iter);
        InetAddr other = handler->get_address();

	if (inet_addr == other)
	  handler->get_alias(&other);

        if (other.sin_port != 0) {
          if ((iter = m_handler_map.find(other)) != m_handler_map.end())
            m_handler_map.erase(iter);
          else {
            HT_ERRORF("Unable to find mapping for %s in HandlerMap",
                      InetAddr::format(other).c_str());
          }
        }
      }
      else if ((iter = m_datagram_handler_map.find(inet_addr))
                != m_datagram_handler_map.end()) {
        handler = (*iter).second;
        m_datagram_handler_map.erase(iter);
      }
      else
	return Error::COMM_NOT_CONNECTED;
      return Error::OK;
    }

    bool decomission_handler(const CommAddress &addr, IOHandlerPtr &handler) {
      ScopedLock lock(m_mutex);

      if (remove_handler(addr, handler) == Error::OK) {
        m_decomissioned_handlers.insert(handler);
        return true;
      }
      return false;
    }

    bool decomission_handler(const CommAddress &addr) {
      IOHandlerPtr handler;
      return decomission_handler(addr, handler);
    }

    bool translate_proxy_address(const CommAddress &proxy_addr, CommAddress &addr) {
      InetAddr inet_addr;
      HT_ASSERT(proxy_addr.is_proxy());
      if (!m_proxy_map.get_mapping(proxy_addr.proxy, inet_addr))
        return false;
      addr.set_inet(inet_addr);
      return true;
    }

    void purge_handler(IOHandler *handler) {
      ScopedLock lock(m_mutex);
      m_decomissioned_handlers.erase(handler);
      if (m_decomissioned_handlers.empty())
        m_cond.notify_all();
    }

    void decomission_all(std::set<IOHandler *> &handlers) {
      ScopedLock lock(m_mutex);
      SockAddrMap<IOHandlerPtr>::iterator iter;

      // TCP handlers
      for (iter = m_handler_map.begin(); iter != m_handler_map.end(); ++iter) {
        m_decomissioned_handlers.insert((*iter).second);
        handlers.insert((*iter).second.get());
      }
      m_handler_map.clear();

      // UDP handlers
      for (iter = m_datagram_handler_map.begin();
           iter != m_datagram_handler_map.end(); ++iter) {
        m_decomissioned_handlers.insert((*iter).second);
        handlers.insert((*iter).second.get());
      }
      m_datagram_handler_map.clear();
    }

    void wait_for_empty() {
      ScopedLock lock(m_mutex);
      if (!m_decomissioned_handlers.empty())
        m_cond.wait(lock);
    }

    int propagate_mappings(ProxyMapT &mappings) {
      int last_error = Error::OK;

      if (mappings.empty())
	return Error::OK;

      SockAddrMap<IOHandlerPtr>::iterator iter;
      String mapping;

      foreach(const ProxyMapT::value_type &v, mappings)
	mapping += v.first + "\t" + InetAddr::format(v.second) + "\n";

      uint8_t *buffer = new uint8_t [ mapping.length() + 1 ];
      strcpy((char *)buffer, mapping.c_str());
      boost::shared_array<uint8_t> payload(buffer);
      CommHeader header;
      header.flags |= CommHeader::FLAGS_BIT_PROXY_MAP_UPDATE;
      for (iter = m_handler_map.begin(); iter != m_handler_map.end(); ++iter) {
	IOHandlerData *io_handler_data = dynamic_cast<IOHandlerData *>((*iter).second.get());
	if (io_handler_data) {
	  CommBufPtr comm_buf = new CommBuf(header, 0, payload, mapping.length()+1);
	  comm_buf->write_header_and_reset();
	  int error = io_handler_data->send_message(comm_buf);
	  if (error != Error::OK) {
	    HT_ERRORF("Unable to propagate proxy mappings to %s - %s",
		      InetAddr(io_handler_data->get_address()).format().c_str(),
		      Error::get_text(error));
	    last_error = error;
	  }
	}
      }
      return last_error;
    }

  private:

    /**
     * Translates CommAddress into INET socket address
     */
    int translate_address(const CommAddress &addr, InetAddr *inet_addr) {

      HT_ASSERT(addr.is_set());

      if (addr.is_proxy()) {
	if (!m_proxy_map.get_mapping(addr.proxy, *inet_addr))
	  return Error::COMM_INVALID_PROXY;
      }
      else
	memcpy(inet_addr, &addr.inet, sizeof(InetAddr));

      return Error::OK;
    }

    IOHandler *lookup_handler(const InetAddr &addr) {
      SockAddrMap<IOHandlerPtr>::iterator iter = m_handler_map.find(addr);
      if (iter == m_handler_map.end())
        return 0;
      return (*iter).second.get();
    }

    Mutex                      m_mutex;
    boost::condition           m_cond;
    SockAddrMap<IOHandlerPtr>  m_handler_map;
    SockAddrMap<IOHandlerPtr>  m_datagram_handler_map;
    std::set<IOHandlerPtr, ltiohp>  m_decomissioned_handlers;
    ProxyMap                   m_proxy_map;
    bool                       m_proxies_loaded;
  };
  typedef boost::intrusive_ptr<HandlerMap> HandlerMapPtr;

}


#endif // HYPERTABLE_HANDLERMAP_H
