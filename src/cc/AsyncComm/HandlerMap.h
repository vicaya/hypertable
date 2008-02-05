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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HYPERTABLE_HANDLERMAP_H
#define HYPERTABLE_HANDLERMAP_H

#include <cassert>

//#define DISABLE_LOG_DEBUG

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

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
      boost::mutex::scoped_lock lock(m_mutex);
      assert(m_handler_map.find(handler->get_address()) == m_handler_map.end());
      m_handler_map[handler->get_address()] = handler;
    }

    int set_alias(struct sockaddr_in &addr, struct sockaddr_in &alias) {
      boost::mutex::scoped_lock lock(m_mutex);
      SockAddrMapT<IOHandlerPtr>::iterator iter;

      if (m_handler_map.find(alias) != m_handler_map.end())
	return Error::COMM_CONFLICTING_ADDRESS;

      if ((iter = m_handler_map.find(addr)) == m_handler_map.end())
	return Error::COMM_NOT_CONNECTED;

      (*iter).second->set_alias(alias);
      m_handler_map[alias] = (*iter).second;

      return Error::OK;
    }

    bool contains_handler(struct sockaddr_in &addr) {
      boost::mutex::scoped_lock lock(m_mutex);
      return lookup_handler(addr);
    }

    bool lookup_data_handler(struct sockaddr_in &addr, IOHandlerDataPtr &ioHandlerDataPtr) {
      boost::mutex::scoped_lock lock(m_mutex);
      IOHandler *handler = lookup_handler(addr);
      if (handler) {
	ioHandlerDataPtr = dynamic_cast<IOHandlerData *>(handler);
	if (ioHandlerDataPtr)
	  return true;
      }
      return false;
    }

    void insert_datagram_handler(IOHandler *handler) {
      boost::mutex::scoped_lock lock(m_mutex);
      assert(m_datagram_handler_map.find(handler->get_local_address()) == m_datagram_handler_map.end());
      m_datagram_handler_map[handler->get_local_address()] = handler;
    }

    bool lookup_datagram_handler(struct sockaddr_in &addr, IOHandlerDatagramPtr &ioHandlerDatagramPtr) {
      boost::mutex::scoped_lock lock(m_mutex);
      SockAddrMapT<IOHandlerPtr>::iterator iter = m_datagram_handler_map.find(addr);
      if (iter == m_handler_map.end())
	return false;
      ioHandlerDatagramPtr = (IOHandlerDatagram *)(*iter).second.get();
      return true;
    }

    bool remove_handler(struct sockaddr_in &addr, IOHandlerPtr &handlerPtr) {
      SockAddrMapT<IOHandlerPtr>::iterator iter;
      if ((iter = m_handler_map.find(addr)) != m_handler_map.end()) {
	handlerPtr = (*iter).second;
	m_handler_map.erase(iter);
	struct sockaddr_in alias;
	handlerPtr->get_alias(&alias);
	if (alias.sin_port != 0) {
	  if ((iter = m_handler_map.find(alias)) != m_handler_map.end())
	    m_handler_map.erase(iter);
	  else {
	    std::string err_str;
	    HT_ERRORF("Unable to find mapping for alias (%s) in HandlerMap", InetAddr::string_format(err_str, alias));
	  }
	}
      }
      else if ((iter = m_datagram_handler_map.find(addr)) != m_datagram_handler_map.end()) {
	handlerPtr = (*iter).second;
	m_datagram_handler_map.erase(iter);
      }
      else
	return false;
      return true;
    }

    bool decomission_handler(struct sockaddr_in &addr, IOHandlerPtr &handlerPtr) {
      boost::mutex::scoped_lock lock(m_mutex);

      if (remove_handler(addr, handlerPtr)) {
	m_decomissioned_handlers.insert(handlerPtr);
	return true;
      }
      return false;
    }

    bool decomission_handler(struct sockaddr_in &addr) {
      IOHandlerPtr handlerPtr;
      return decomission_handler(addr, handlerPtr);
    }

    void purge_handler(IOHandler *handler) {
      boost::mutex::scoped_lock lock(m_mutex);
      IOHandlerPtr handlerPtr = handler;
      m_decomissioned_handlers.erase(handlerPtr);
      if (m_decomissioned_handlers.empty())
	m_cond.notify_all();
    }

    void decomission_all(set<IOHandler *> &handlers) {
      boost::mutex::scoped_lock lock(m_mutex);
      SockAddrMapT<IOHandlerPtr>::iterator iter;
      
      // TCP handlers
      for (iter = m_handler_map.begin(); iter != m_handler_map.end(); iter++) {
	m_decomissioned_handlers.insert((*iter).second);
	handlers.insert((*iter).second.get());
      }
      m_handler_map.clear();

      // UDP handlers
      for (iter = m_datagram_handler_map.begin(); iter != m_datagram_handler_map.end(); iter++) {
	m_decomissioned_handlers.insert((*iter).second);
	handlers.insert((*iter).second.get());
      }
      m_datagram_handler_map.clear();
    }

    void wait_for_empty() {
      boost::mutex::scoped_lock lock(m_mutex);
      if (!m_decomissioned_handlers.empty())
	m_cond.wait(lock);
    }

  private:

    IOHandler *lookup_handler(struct sockaddr_in &addr) {
      SockAddrMapT<IOHandlerPtr>::iterator iter = m_handler_map.find(addr);
      if (iter == m_handler_map.end())
	return 0;
      return (*iter).second.get();
    }

    boost::mutex                m_mutex;
    boost::condition            m_cond;
    SockAddrMapT<IOHandlerPtr>  m_handler_map;
    SockAddrMapT<IOHandlerPtr>  m_datagram_handler_map;
    set<IOHandlerPtr, ltiohp>   m_decomissioned_handlers;
  };
  typedef boost::intrusive_ptr<HandlerMap> HandlerMapPtr;

}


#endif // HYPERTABLE_HANDLERMAP_H
