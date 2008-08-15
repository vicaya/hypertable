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
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/ReferenceCount.h"
#include "Common/SockAddrMap.h"

#include "IOHandlerData.h"
#include "IOHandlerDatagram.h"

namespace Hypertable {

  class HandlerMap : public ReferenceCount {

  public:

    void insert_handler(IOHandler *handler) {
      ScopedLock lock(m_mutex);
      assert(m_handler_map.find(handler->get_address()) == m_handler_map.end());
      m_handler_map[handler->get_address()] = handler;
    }

    int set_alias(const sockaddr_in &addr, const sockaddr_in &alias) {
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

    bool contains_handler(const sockaddr_in &addr) {
      ScopedLock lock(m_mutex);
      return lookup_handler(addr);
    }

    bool lookup_data_handler(const sockaddr_in &addr,
                             IOHandlerDataPtr &io_handler_data) {
      ScopedLock lock(m_mutex);
      IOHandler *handler = lookup_handler(addr);
      if (handler) {
        io_handler_data = dynamic_cast<IOHandlerData *>(handler);
        if (io_handler_data)
          return true;
      }
      return false;
    }

    void insert_datagram_handler(IOHandler *handler) {
      ScopedLock lock(m_mutex);
      HT_EXPECT(m_datagram_handler_map.find(handler->get_local_address())
                == m_datagram_handler_map.end(), Error::FAILED_EXPECTATION);
      m_datagram_handler_map[handler->get_local_address()] = handler;
    }

    bool lookup_datagram_handler(const sockaddr_in &addr,
                                 IOHandlerDatagramPtr &io_handler_dg) {
      ScopedLock lock(m_mutex);
      SockAddrMap<IOHandlerPtr>::iterator iter =
          m_datagram_handler_map.find(addr);

      if (iter == m_handler_map.end())
        return false;

      io_handler_dg = (IOHandlerDatagram *)(*iter).second.get();

      return true;
    }

    bool remove_handler(const sockaddr_in &addr, IOHandlerPtr &handler) {
      SockAddrMap<IOHandlerPtr>::iterator iter;

      if ((iter = m_handler_map.find(addr)) != m_handler_map.end()) {
        handler = (*iter).second;
        m_handler_map.erase(iter);
        struct sockaddr_in alias;
        handler->get_alias(&alias);

        if (alias.sin_port != 0) {
          if ((iter = m_handler_map.find(alias)) != m_handler_map.end())
            m_handler_map.erase(iter);
          else {
            HT_ERRORF("Unable to find mapping for alias (%s) in HandlerMap",
                      InetAddr::format(alias).c_str());
          }
        }
      }
      else if ((iter = m_datagram_handler_map.find(addr))
                != m_datagram_handler_map.end()) {
        handler = (*iter).second;
        m_datagram_handler_map.erase(iter);
      }
      else
        return false;
      return true;
    }

    bool decomission_handler(const sockaddr_in &addr, IOHandlerPtr &handler) {
      ScopedLock lock(m_mutex);

      if (remove_handler(addr, handler)) {
        m_decomissioned_handlers.insert(handler);
        return true;
      }
      return false;
    }

    bool decomission_handler(const sockaddr_in &addr) {
      IOHandlerPtr handler;
      return decomission_handler(addr, handler);
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

  private:

    IOHandler *lookup_handler(const sockaddr_in &addr) {
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
  };
  typedef boost::intrusive_ptr<HandlerMap> HandlerMapPtr;

}


#endif // HYPERTABLE_HANDLERMAP_H
