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

ClientKeepaliveHandler::ClientKeepaliveHandler(Comm *comm, PropertiesPtr &cfg,
                                               Session *session)
  : m_dead(false), m_comm(comm), m_session(session), m_session_id(0),
    m_last_known_event(0) {
  int error;
  uint16_t master_port;
  String master_host;

  HT_TRY("getting config values",
    m_verbose = cfg->get_bool("Hypertable.Verbose");
    master_host = cfg->get_str("Hyperspace.Master.Host");
    master_port = cfg->get_i16("Hyperspace.Master.Port");
    m_lease_interval = cfg->get_i32("Hyperspace.Lease.Interval");
    m_keep_alive_interval = cfg->get_i32("Hyperspace.KeepAlive.Interval");
    m_reconnect = cfg->get_bool("Hyperspace.Session.Reconnect"));

  HT_EXPECT(InetAddr::initialize(&m_master_addr, master_host.c_str(),
            master_port), Error::BAD_DOMAIN_NAME);

  boost::xtime_get(&m_last_keep_alive_send_time, boost::TIME_UTC);
  boost::xtime_get(&m_jeopardy_time, boost::TIME_UTC);
  xtime_add_millis(m_jeopardy_time, m_lease_interval);

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

  if ((error = m_comm->set_timer(m_keep_alive_interval, this))
      != Error::OK) {
    HT_ERRORF("Problem setting timer - %s", Error::get_text(error));
    exit(1);
  }
}


void ClientKeepaliveHandler::handle(Hypertable::EventPtr &event) {
  ScopedLock lock(m_mutex);
  int error;

  if (m_dead)
    return;

  /**
  if (m_verbose) {
    HT_INFOF("%s", event->to_str().c_str());
  }
  **/

  if (event->type == Hypertable::Event::MESSAGE) {
    const uint8_t *decode_ptr = event->payload;
    size_t decode_remain = event->payload_len;

    try {

      // sanity check command code
      if (event->header.command >= Protocol::COMMAND_MAX)
        HT_THROWF(Error::PROTOCOL_ERROR, "Invalid command (%llu)",
                  (Llu)event->header.command);

      switch (event->header.command) {
      case Protocol::COMMAND_KEEPALIVE:
        {
          uint64_t session_id;
          uint32_t notifications;
          uint64_t handle, event_id;
          uint32_t event_mask;
          const char *name;
          const uint8_t *post_notification_buf;
          size_t post_notification_size;

          if (m_session->get_state() == Session::STATE_EXPIRED)
            return;

          // update jeopardy time
          memcpy(&m_jeopardy_time, &m_last_keep_alive_send_time,
                 sizeof(boost::xtime));
          xtime_add_millis(m_jeopardy_time, m_lease_interval);

          session_id = decode_i64(&decode_ptr, &decode_remain);
          error = decode_i32(&decode_ptr, &decode_remain);

          if (error != Error::OK) {
            HT_ERRORF("Master session error - %s", Error::get_text(error));
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

          notifications = decode_i32(&decode_ptr, &decode_remain);

          // Start Issue 313 instrumentation
          post_notification_buf = decode_ptr;
          post_notification_size = decode_remain;

          for (uint32_t i=0; i<notifications; i++) {
            handle = decode_i64(&decode_ptr, &decode_remain);
            event_id = decode_i64(&decode_ptr, &decode_remain);
            event_mask = decode_i32(&decode_ptr, &decode_remain);

            HandleMap::iterator iter = m_handle_map.find(handle);
            if (iter == m_handle_map.end()) {
              // We have a bad notification, ie. a notification for a handle not in m_handle_map
              boost::xtime now;
              boost::xtime_get(&now, boost::TIME_UTC);

              HT_ERROR_OUT << "[Issue 313] Received bad notification session=" << m_session_id
                           << ", handle=" << handle << ", event_id=" << event_id
                           << ", event_mask=" << event_mask << HT_END;
              // ignore all notifications in this keepalive message (don't kick up to
              // application) this avoids multiple notifications being sent to app (Issue 314)
              notifications=0;

              // Check if we already have a pending bad notification for this handle
              BadNotificationHandleMap::iterator uiter = m_bad_handle_map.find(handle);
              if (uiter == m_bad_handle_map.end()) {
                // if not then store
                m_bad_handle_map[handle] = now;
              }
              else {
                // if we do then check against grace period
                uint64_t time_diff=xtime_diff_millis((*uiter).second ,now);
                if (time_diff > ms_bad_notification_grace_period) {
                  HT_ERROR_OUT << "[Issue 313] Still receiving bad notification after grace "
                               << "period=" << ms_bad_notification_grace_period
                               << "ms, session=" << m_session_id
                               << ", handle=" << handle << ", event_id=" << event_id
                               << ", event_mask=" << event_mask << HT_END;
                  HT_ASSERT(false);
                }
              }
              break;
            }
            else {
              // This is a good notification, clear any prev bad notifications for this handle
              BadNotificationHandleMap::iterator uiter = m_bad_handle_map.find(handle);
              if (uiter != m_bad_handle_map.end()) {
                HT_ERROR_OUT << "[Issue 313] Previously bad notification cleared within grace "
                             << "period=" << ms_bad_notification_grace_period
                             << "ms, session=" << m_session_id
                             << ", handle=" << handle << ", event_id=" << event_id
                             << ", event_mask=" << event_mask << HT_END;
                m_bad_handle_map.erase(uiter);
              }
            }

            if (event_mask == EVENT_MASK_ATTR_SET ||
                event_mask == EVENT_MASK_ATTR_DEL ||
                event_mask == EVENT_MASK_CHILD_NODE_ADDED ||
                event_mask == EVENT_MASK_CHILD_NODE_REMOVED)
              decode_vstr(&decode_ptr, &decode_remain);
            else if (event_mask == EVENT_MASK_LOCK_ACQUIRED)
              decode_i32(&decode_ptr, &decode_remain);
            else if (event_mask == EVENT_MASK_LOCK_GRANTED) {
              decode_i32(&decode_ptr, &decode_remain);
              decode_i64(&decode_ptr, &decode_remain);
            }
          }

          decode_ptr = post_notification_buf;
          decode_remain = post_notification_size;
          // End Issue 313 instrumentation

          for (uint32_t i=0; i<notifications; i++) {
            handle = decode_i64(&decode_ptr, &decode_remain);
            event_id = decode_i64(&decode_ptr, &decode_remain);
            event_mask = decode_i32(&decode_ptr, &decode_remain);

            HandleMap::iterator iter = m_handle_map.find(handle);
            if (iter == m_handle_map.end()) {
              HT_ERROR_OUT << "[Issue 313] this should never happen bad notification session="
                  << m_session_id << ", handle=" << handle << ", event_id=" << event_id
                  << ", event_mask=" << event_mask << HT_END;
              assert(false);
            }

            ClientHandleStatePtr handle_state = (*iter).second;

            if (event_mask == EVENT_MASK_ATTR_SET ||
                event_mask == EVENT_MASK_ATTR_DEL ||
                event_mask == EVENT_MASK_CHILD_NODE_ADDED ||
                event_mask == EVENT_MASK_CHILD_NODE_REMOVED) {
              name = decode_vstr(&decode_ptr, &decode_remain);

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
              uint32_t mode = decode_i32(&decode_ptr, &decode_remain);

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
              uint32_t mode = decode_i32(&decode_ptr, &decode_remain);
              handle_state->lock_generation = decode_i64(&decode_ptr,
                                                         &decode_remain);
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

          m_session->advance_expire_time(m_last_keep_alive_send_time);

          assert(m_session_id == session_id);
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

    if ((error = m_comm->set_timer(m_keep_alive_interval, this))
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

  if (m_reconnect) {
    int error;
    m_conn_handler_ptr = 0;
    m_handle_map.clear();
    m_bad_handle_map.clear();
    m_session_id = 0;
    m_last_known_event = 0;

    m_session->state_transition(Session::STATE_DISCONNECTED);

    if (m_conn_handler_ptr)
      m_conn_handler_ptr->close();
    poll(0,0,2000);
    boost::xtime_get(&m_last_keep_alive_send_time, boost::TIME_UTC);
    boost::xtime_get(&m_jeopardy_time, boost::TIME_UTC);
    xtime_add_millis(m_jeopardy_time, m_lease_interval);

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

    if ((error = m_comm->set_timer(m_keep_alive_interval, this))
        != Error::OK) {
      HT_ERRORF("Problem setting timer - %s", Error::get_text(error));
      exit(1);
    }
  }
  else {
    m_session->state_transition(Session::STATE_EXPIRED);
    if (m_conn_handler_ptr)
      m_conn_handler_ptr->close();
    poll(0,0,2000);
    m_conn_handler_ptr = 0;
    m_handle_map.clear();
    m_bad_handle_map.clear();
    m_session_id = 0;
    m_last_known_event = 0;
  }

  return;
}


void ClientKeepaliveHandler::destroy_session() {
  int error;

  CommBufPtr cbp(Hyperspace::Protocol::create_client_keepalive_request(
                 m_session_id, m_last_known_event, true));

  if ((error = m_comm->send_datagram(m_master_addr, m_local_addr, cbp)
      != Error::OK))
    HT_ERRORF("Unable to send datagram - %s", Error::get_text(error));

  m_dead = true;
  //m_comm->close_socket(m_local_addr);
}

