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

#include "Common/Compat.h"
extern "C" {
#include <poll.h>
}

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/StringExt.h"

#include "ClientKeepaliveHandler.h"
#include "Master.h"
#include "Protocol.h"
#include "Session.h"

using namespace std;
using namespace Hypertable;
using namespace Hyperspace;
using namespace Serialization;

ClientKeepaliveHandler::ClientKeepaliveHandler(Comm *comm, PropertiesPtr &props_ptr, Session *session) : m_dead(false), m_comm(comm), m_session(session), m_session_id(0), m_last_known_event(0) {
  int error;
  uint16_t master_port;
  const char *master_host;

  m_verbose = props_ptr->get_bool("Hypertable.Verbose", false);
  master_host = props_ptr->get("Hyperspace.Master.Host", "localhost");
  master_port = (uint16_t)props_ptr->get_int("Hyperspace.Master.Port", Master::DEFAULT_MASTER_PORT);
  m_lease_interval = (uint32_t)props_ptr->get_int("Hyperspace.Lease.Interval", Master::DEFAULT_LEASE_INTERVAL);
  m_keep_alive_interval = (uint32_t)props_ptr->get_int("Hyperspace.KeepAlive.Interval", Master::DEFAULT_KEEPALIVE_INTERVAL);

  if (!InetAddr::initialize(&m_master_addr, master_host, master_port))
    exit(1);

  if (m_verbose) {
    cout << "Hyperspace.KeepAlive.Interval=" << m_keep_alive_interval << endl;
    cout << "Hyperspace.Lease.Interval=" << m_lease_interval << endl;
    cout << "Hyperspace.Master.Host=" << master_host << endl;
    cout << "Hyperspace.Master.Port=" << master_port << endl;
  }

  boost::xtime_get(&m_last_keep_alive_send_time, boost::TIME_UTC);
  boost::xtime_get(&m_jeopardy_time, boost::TIME_UTC);
  m_jeopardy_time.sec += m_lease_interval;

  InetAddr::initialize(&m_local_addr, INADDR_ANY, 0);

  DispatchHandlerPtr dhp(this);
  m_comm->create_datagram_receive_socket(&m_local_addr, 0x10, dhp);

  CommBufPtr cbp(Hyperspace::Protocol::create_client_keepalive_request(
      m_session_id, m_last_known_event));

  if ((error = m_comm->send_datagram(m_master_addr, m_local_addr, cbp)
      != Error::OK)) {
    HT_ERRORF("Unable to send datagram - %s", Error::get_text(error));
    exit(1);
  }

  if ((error = m_comm->set_timer(m_keep_alive_interval*1000, this))
      != Error::OK) {
    HT_ERRORF("Problem setting timer - %s", Error::get_text(error));
    exit(1);
  }
}



/**
 *
 */
void ClientKeepaliveHandler::handle(Hypertable::EventPtr &event) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  int command = -1;

  if (m_dead)
    return;

  /**
  if (m_verbose) {
    HT_INFOF("%s", event->to_str().c_str());
  }
  **/

  if (event->type == Hypertable::Event::MESSAGE) {
    const uint8_t *msg = event->message;
    size_t remaining = event->message_len;

    try {
      command = decode_i16(&msg, &remaining);

      // sanity check command code
      if (command >= Protocol::COMMAND_MAX)
        HT_THROWF(Error::PROTOCOL_ERROR, "Invalid command (%d)", command);

      switch (command) {
      case Protocol::COMMAND_KEEPALIVE:
        {
          uint64_t session_id;
          int state;
          uint32_t notifications;
          uint64_t handle, event_id;
          uint32_t event_mask;
          const char *name;

          if (m_session->get_state() == Session::STATE_EXPIRED)
            return;

          // update jeopardy time
          memcpy(&m_jeopardy_time, &m_last_keep_alive_send_time,
                 sizeof(boost::xtime));
          m_jeopardy_time.sec += m_lease_interval;

          session_id = decode_i64(&msg, &remaining);
          error = decode_i32(&msg, &remaining);

          if (error != Error::OK) {
            if (error != Error::HYPERSPACE_EXPIRED_SESSION) {
              HT_ERRORF("Master session error - %s", Error::get_text(error));
            }
            expire_session();
            return;
          }

          if (m_session_id == 0) {
            m_session_id = session_id;
            if (!m_conn_handler_ptr) {
              m_conn_handler_ptr = new ClientConnectionHandler(m_comm,
                  m_session, m_lease_interval);
              m_conn_handler_ptr->set_verbose_mode(m_verbose);
              m_conn_handler_ptr->set_session_id(m_session_id);
            }
          }

          notifications = decode_i32(&msg, &remaining);

          for (uint32_t i=0; i<notifications; i++) {
            handle = decode_i64(&msg, &remaining);
            event_id = decode_i64(&msg, &remaining);
            event_mask = decode_i32(&msg, &remaining);

            HandleMap::iterator iter = m_handle_map.find(handle);
            assert (iter != m_handle_map.end());
            ClientHandleStatePtr handle_state = (*iter).second;

            if (event_mask == EVENT_MASK_ATTR_SET ||
                event_mask == EVENT_MASK_ATTR_DEL ||
                event_mask == EVENT_MASK_CHILD_NODE_ADDED ||
                event_mask == EVENT_MASK_CHILD_NODE_REMOVED) {
              name = decode_vstr(&msg, &remaining);

              if (event_id <= m_last_known_event)
                continue;

              if (handle_state->callback) {
                if (event_mask == EVENT_MASK_ATTR_SET)
                  handle_state->callback->attr_set(name);
                else if (event_mask == EVENT_MASK_ATTR_DEL)
                  handle_state->callback->attr_del(name);
                else if (event_mask == EVENT_MASK_CHILD_NODE_ADDED)
                  handle_state->callback->child_node_added(name);
                else
                  handle_state->callback->child_node_removed(name);
              }
            }
            else if (event_mask == EVENT_MASK_LOCK_ACQUIRED) {
              uint32_t mode = decode_i32(&msg, &remaining);

              if (event_id <= m_last_known_event)
                continue;
              if (handle_state->callback)
                handle_state->callback->lock_acquired(mode);
            }
            else if (event_mask == EVENT_MASK_LOCK_RELEASED) {
              if (event_id <= m_last_known_event)
                continue;
              if (handle_state->callback)
                handle_state->callback->lock_released();
            }
            else if (event_mask == EVENT_MASK_LOCK_GRANTED) {
              uint32_t mode = decode_i32(&msg, &remaining);
              handle_state->lock_generation = decode_i64(&msg, &remaining);

              if (event_id <= m_last_known_event)
                continue;

              handle_state->lock_status = LOCK_STATUS_GRANTED;
              handle_state->sequencer->generation =
                  handle_state->lock_generation;
              handle_state->sequencer->mode = mode;
              handle_state->cond.notify_all();
            }

            m_last_known_event = event_id;
          }

          /**
          if (m_verbose) {
            HT_INFOF("session_id = %lld", m_session_id);
          }
          **/

          if (m_conn_handler_ptr->disconnected())
            m_conn_handler_ptr->initiate_connection(m_master_addr);
          else
            state = m_session->state_transition(Session::STATE_SAFE);

          if (notifications > 0) {
            CommBufPtr cbp(Protocol::create_client_keepalive_request(
                m_session_id, m_last_known_event));
            boost::xtime_get(&m_last_keep_alive_send_time, boost::TIME_UTC);
            if ((error = m_comm->send_datagram(m_master_addr, m_local_addr, cbp)
                != Error::OK)) {
              HT_ERRORF("Unable to send datagram - %s", Error::get_text(error));
              exit(1);
            }
          }

          assert(m_session_id == session_id);
        }
        break;
      default:
        HT_THROWF(Error::PROTOCOL_ERROR, "Unimplemented command (%d)", command);
      }
    }
    catch (Exception &e) {
      HT_ERROR_OUT << e << HT_END;
    }
  }
  else if (event->type == Hypertable::Event::TIMER) {
    boost::xtime now;
    int state;

    // !!! fix - what about re-ordered packets?

    if ((state = m_session->get_state()) == Session::STATE_EXPIRED)
      return;

    boost::xtime_get(&now, boost::TIME_UTC);

    if (state == Session::STATE_SAFE) {
      if (xtime_cmp(m_jeopardy_time, now) < 0) {
        m_session->state_transition(Session::STATE_JEOPARDY);
      }
    }
    else if (m_session->expired()) {
      expire_session();
      return;
    }

    CommBufPtr cbp(Hyperspace::Protocol::create_client_keepalive_request(
        m_session_id, m_last_known_event));

    boost::xtime_get(&m_last_keep_alive_send_time, boost::TIME_UTC);

    if ((error = m_comm->send_datagram(m_master_addr, m_local_addr, cbp)
        != Error::OK)) {
      HT_ERRORF("Unable to send datagram - %s", Error::get_text(error));
      exit(1);
    }

    if ((error = m_comm->set_timer(m_keep_alive_interval*1000, this))
        != Error::OK) {
      HT_ERRORF("Problem setting timer - %s", Error::get_text(error));
      exit(1);
    }
  }
  else {
    HT_INFOF("%s", event->to_str().c_str());
  }
}



void ClientKeepaliveHandler::expire_session() {
  m_session->state_transition(Session::STATE_EXPIRED);
  if (m_conn_handler_ptr)
    m_conn_handler_ptr->close();
  poll(0,0,2000);
  m_conn_handler_ptr = 0;
  return;
}


void ClientKeepaliveHandler::destroy_session() {
  int error;

  CommBufPtr cbp(Hyperspace::Protocol::create_client_keepalive_request(m_session_id, m_last_known_event, true));

  if ((error = m_comm->send_datagram(m_master_addr, m_local_addr, cbp) != Error::OK))
    HT_ERRORF("Unable to send datagram - %s", Error::get_text(error));

  m_dead = true;
  //m_comm->close_socket(m_local_addr);

}
