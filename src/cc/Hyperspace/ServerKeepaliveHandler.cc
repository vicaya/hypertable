/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include "Common/InetAddr.h"
#include "Common/StringExt.h"
#include "Common/System.h"

#include "RequestHandlerRenewSession.h"
#include "RequestHandlerExpireSessions.h"
#include "ServerKeepaliveHandler.h"
#include "Master.h"
#include "Protocol.h"
#include "SessionData.h"

using namespace Hypertable;
using namespace Hyperspace;
using namespace Serialization;


ServerKeepaliveHandler::ServerKeepaliveHandler(Comm *comm, Master *master,
                                               ApplicationQueuePtr &app_queue)
  : m_comm(comm), m_master(master), m_app_queue_ptr(app_queue) {
  int error;

  m_master->get_datagram_send_address(&m_send_addr);

  if ((error = m_comm->set_timer(Master::TIMER_INTERVAL_MS, this)) != Error::OK) {
    HT_ERRORF("Problem setting timer - %s", Error::get_text(error));
    exit(1);
  }

  m_master->tick();

}


void ServerKeepaliveHandler::handle(Hypertable::EventPtr &event) {
  int error;

  if (event->type == Hypertable::Event::MESSAGE) {
    const uint8_t *decode_ptr = event->payload;
    size_t decode_remain = event->payload_len;

    try {

      // sanity check command code
      if (event->header.command >= Protocol::COMMAND_MAX)
        HT_THROWF(Error::PROTOCOL_ERROR, "Invalid command (%llu)",
                  (Llu)event->header.command);

      switch (event->header.command) {
      case Protocol::COMMAND_KEEPALIVE: {
          uint64_t session_id = decode_i64(&decode_ptr, &decode_remain);
          uint64_t last_known_event = decode_i64(&decode_ptr, &decode_remain);
          bool shutdown = decode_bool(&decode_ptr, &decode_remain);

          m_app_queue_ptr->add( new RequestHandlerRenewSession(m_comm,
              m_master, session_id, last_known_event, shutdown, event,
              &m_send_addr ) );
        }
        break;
      default:
        HT_THROWF(Error::PROTOCOL_ERROR, "Unimplemented command (%llu)",
                  (Llu)event->header.command);
      }
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }
  }
  else if (event->type == Hypertable::Event::TIMER) {

    m_master->tick();

    try {
      m_app_queue_ptr->add( new RequestHandlerExpireSessions(m_master) );
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }

    if ((error = m_comm->set_timer(Master::TIMER_INTERVAL_MS, this)) != Error::OK) {
      HT_ERRORF("Problem setting timer - %s", Error::get_text(error));
      exit(1);
    }

  }
  else {
    HT_INFOF("%s", event->to_str().c_str());
  }
}


void ServerKeepaliveHandler::deliver_event_notifications(uint64_t session_id) {
  int error = 0;
  SessionDataPtr session_ptr;

  //HT_INFOF("Delivering event notifications for session %lld", session_id);

  if (!m_master->get_session(session_id, session_ptr)) {
    HT_ERRORF("Unable to find data for session %llu", (Llu)session_id);
    return;
  }

  /**
  HT_INFOF("Sending Keepalive request to %s",
           InetAddr::format(session_ptr->addr));
  **/

  CommBufPtr cbp(Protocol::create_server_keepalive_request(session_ptr));

  if ((error = m_comm->send_datagram(session_ptr->addr, m_send_addr, cbp))
      != Error::OK) {
    HT_ERRORF("Comm::send_datagram returned %s", Error::get_text(error));
  }
}
