/**
 * Copyright (C) 2009 Sanjit Jhala (Zvents, Inc.)
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

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/InetAddr.h"

#include "AsyncComm/CommBuf.h"

#include "Protocol.h"
#include "Master.h"
#include "RequestHandlerRenewSession.h"
#include "SessionData.h"

using namespace Hyperspace;
using namespace Hypertable;

/**
 *
 */
void RequestHandlerRenewSession::run() {

  int error;

  try {
    SessionDataPtr session_ptr;

    if (m_shutdown) {
      m_master->destroy_session(m_session_id);
      return;
    }

    // if this is not the current replication master then try to return
    // addr of current master

    if (!m_master->is_master()) {
      String location = m_master->get_current_master();

      HT_DEBUG_OUT << "Redirecting request to current master " << location << HT_END;

      CommBufPtr cbp(Protocol::create_server_redirect_request(location));
      error = m_comm->send_datagram(m_event->addr, *m_send_addr, cbp);
      if (error != Error::OK) {
        HT_ERRORF("Comm::send_datagram returned %s", Error::get_text(error));
      }
      HT_DEBUG_OUT << "Redirected request to current master " << location << HT_END;
      return;
    }

    HT_DEBUG_OUT << "Handling renew session request at local (master) site" << HT_END;

    if (m_session_id == 0) {
      HT_DEBUG_OUT << "Do create session request at local (master) site" << HT_END;
      m_session_id = m_master->create_session(m_event->addr);
      HT_INFOF("Session handle %llu created", (Llu)m_session_id);
      error = Error::OK;
    }
    else
      error = m_master->renew_session_lease(m_session_id);

    if (error == Error::HYPERSPACE_EXPIRED_SESSION) {
      HT_INFOF("Session handle %llu expired", (Llu)m_session_id);
      CommBufPtr cbp(Protocol::create_server_keepalive_request(m_session_id,
                     Error::HYPERSPACE_EXPIRED_SESSION));
      error = m_comm->send_datagram(m_event->addr, *m_send_addr, cbp);
      if (error != Error::OK) {
        HT_ERRORF("Comm::send_datagram returned %s",
                  Error::get_text(error));
      }
      return;
    }

    if (!m_master->get_session(m_session_id, session_ptr)) {
      HT_ERRORF("Unable to find data for session %llu", (Llu)m_session_id);
      return;
    }

    session_ptr->purge_notifications(m_last_known_event);

    /**
    HT_INFOF("Sending Keepalive request to %s (m_last_known_event=%lld)",
             InetAddr::format(m_event->addr), m_last_known_event);
    **/

    CommBufPtr cbp(Protocol::create_server_keepalive_request(
                   session_ptr));
    error = m_comm->send_datagram(m_event->addr, *m_send_addr, cbp);
    if (error != Error::OK) {
      HT_ERRORF("Comm::send_datagram returned %s",
                Error::get_text(error));
    }

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}
