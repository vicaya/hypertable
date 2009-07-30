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

#ifndef HYPERSPACE_CLIENTKEEPALIVEHANDLER_H
#define HYPERSPACE_CLIENTKEEPALIVEHANDLER_H

#include <cassert>

#include <boost/thread/mutex.hpp>
#include <boost/thread/xtime.hpp>

#include "Common/Time.h"

#include "Common/StringExt.h"
#include "Common/Properties.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/DispatchHandler.h"

#include "ClientConnectionHandler.h"
#include "ClientHandleState.h"

namespace Hyperspace {

  class Session;

  /**
   *
   */
  class ClientKeepaliveHandler : public DispatchHandler {

  public:
    ClientKeepaliveHandler(Comm *, PropertiesPtr &, Session *);
    virtual void handle(Hypertable::EventPtr &event_ptr);

    void register_handle(ClientHandleStatePtr &handle_state) {
      ScopedLock lock(m_mutex);
      HandleMap::iterator iter = m_handle_map.find(handle_state->handle);
      assert(iter == m_handle_map.end());
      m_handle_map[handle_state->handle] = handle_state;
    }

    void unregister_handle(uint64_t handle) {
      ScopedLock lock(m_mutex);
      m_handle_map.erase(handle);
    }

    bool get_handle_state(uint64_t handle, ClientHandleStatePtr &handle_state) {
      ScopedLock lock(m_mutex);
      HandleMap::iterator iter = m_handle_map.find(handle);
      if (iter == m_handle_map.end())
        return false;
      handle_state = (*iter).second;
      return true;
    }

    uint64_t get_session_id() {return m_session_id;}
    void expire_session();

    void destroy_session();

  private:

    Mutex              m_mutex;
    boost::xtime       m_last_keep_alive_send_time;
    boost::xtime       m_jeopardy_time;
    bool m_dead;
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
    typedef hash_map<uint64_t, ClientHandleStatePtr> HandleMap;
    HandleMap  m_handle_map;
    typedef hash_map<uint64_t, boost::xtime> BadNotificationHandleMap;
    BadNotificationHandleMap m_bad_handle_map;
    static const uint64_t ms_bad_notification_grace_period = 120000;
    bool m_reconnect;
  };

  typedef intrusive_ptr<ClientKeepaliveHandler> ClientKeepaliveHandlerPtr;

} // namespace Hypertable

#endif // HYPERSPACE_CLIENTKEEPALIVEHANDLER_H

