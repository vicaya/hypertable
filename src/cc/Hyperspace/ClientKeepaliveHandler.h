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

#ifndef HYPERSPACE_CLIENTKEEPALIVEHANDLER_H
#define HYPERSPACE_CLIENTKEEPALIVEHANDLER_H

#include <cassert>
#include <ext/hash_map>

#include <boost/thread/mutex.hpp>

#include "Common/Properties.h"
#include "Common/StringExt.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/DispatchHandler.h"

#include "ClientConnectionHandler.h"
#include "ClientHandleState.h"
#include "HandleCallback.h"

namespace Hyperspace {

  class Session;

  /**
   * 
   */
  class ClientKeepaliveHandler : public DispatchHandler {

  public:
    ClientKeepaliveHandler(Comm *comm, PropertiesPtr &propsPtr, Session *session);
    virtual ~ClientKeepaliveHandler();

    virtual void handle(Hypertable::EventPtr &eventPtr);

    void register_handle(ClientHandleStatePtr &handleStatePtr) {
      boost::mutex::scoped_lock lock(m_mutex);
      HandleMapT::iterator iter = m_handle_map.find(handleStatePtr->handle);
      assert(iter == m_handle_map.end());
      m_handle_map[handleStatePtr->handle] = handleStatePtr;
    }

    void unregister_handle(uint64_t handle) {
      boost::mutex::scoped_lock lock(m_mutex);
      m_handle_map.erase(handle);
    }

    bool get_handle_state(uint64_t handle, ClientHandleStatePtr &handleStatePtr) {
      boost::mutex::scoped_lock lock(m_mutex);
      HandleMapT::iterator iter = m_handle_map.find(handle);
      if (iter == m_handle_map.end())
	return false;
      handleStatePtr = (*iter).second;
      return true;
    }

    void expire_session();

  private:
    boost::mutex       m_mutex;
    boost::xtime       m_last_keep_alive_send_time;
    boost::xtime       m_jeopardy_time;
    Comm *m_comm;
    uint32_t m_lease_interval;
    uint32_t m_keep_alive_interval;
    struct sockaddr_in m_master_addr;
    struct sockaddr_in m_local_addr;
    bool m_verbose;
    Session *m_session;
    uint64_t m_session_id;
    ClientConnectionHandlerPtr m_conn_handler_ptr;
    uint64_t m_last_known_event;

    typedef __gnu_cxx::hash_map<uint64_t, ClientHandleStatePtr> HandleMapT;
    HandleMapT  m_handle_map;
  };
  typedef boost::intrusive_ptr<ClientKeepaliveHandler> ClientKeepaliveHandlerPtr;
  

}

#endif // HYPERSPACE_CLIENTKEEPALIVEHANDLER_H

