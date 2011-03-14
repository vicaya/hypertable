/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
#include "Common/Time.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "Common/Serialization.h"
#include "Common/Timer.h"

#include "Hyperspace/Session.h"

#include "MasterFileHandler.h"
#include "MasterClient.h"
#include "MasterProtocol.h"

using namespace Hypertable;
using namespace Serialization;


MasterClient::MasterClient(ConnectionManagerPtr &conn_mgr,
    Hyperspace::SessionPtr &hyperspace, const String &toplevel_dir,
    uint32_t timeout_ms, ApplicationQueuePtr &app_queue)
  : m_verbose(true), m_conn_manager_ptr(conn_mgr),
    m_hyperspace(hyperspace), m_app_queue_ptr(app_queue),
    m_hyperspace_init(false), m_hyperspace_connected(true),
    m_timeout_ms(timeout_ms), m_toplevel_dir(toplevel_dir) {

  m_comm = m_conn_manager_ptr->get_comm();
  memset(&m_master_addr, 0, sizeof(m_master_addr));

  /**
   * Open toplevel_dir + /master Hyperspace file to discover the master.
   */
  m_master_file_callback_ptr = new MasterFileHandler(this, m_app_queue_ptr);
  m_master_file_handle = 0;

  // register hyperspace session callback
  m_hyperspace_session_callback.m_masterclient = this;
  m_hyperspace->add_callback(&m_hyperspace_session_callback);

  // no need to serialize access in ctor
  initialize_hyperspace();
}

MasterClient::~MasterClient() {
  if (m_master_file_handle != 0)
    m_hyperspace->close(m_master_file_handle);
  m_hyperspace->remove_callback(&m_hyperspace_session_callback);
}


void MasterClient::initiate_connection(DispatchHandlerPtr dhp) {
  m_dispatcher_handler_ptr = dhp;
  if (m_master_file_handle == 0)
    HT_THROW(Error::MASTER_NOT_RUNNING,
             "MasterClient unable to connect to Master");
  reload_master();
}

void MasterClient::hyperspace_disconnected()
{
  ScopedLock lock(m_hyperspace_mutex);
  HT_DEBUG_OUT << "Hyperspace disconnected" << HT_END;
  m_hyperspace_init = false;
  m_hyperspace_connected = false;
}

void MasterClient::hyperspace_reconnected()
{
  ScopedLock lock(m_hyperspace_mutex);
  HT_DEBUG_OUT << "Hyperspace reconnected" << HT_END;
  HT_ASSERT(!m_hyperspace_init);
  m_hyperspace_connected = true;
}

/**
 * Assumes access is serialized via m_hyperspace_mutex
 */
void MasterClient::initialize_hyperspace() {

  if (m_hyperspace_init)
    return;
  HT_ASSERT(m_hyperspace_connected);

  try {
    Timer timer(m_timeout_ms, true);
    m_master_file_handle = m_hyperspace->open(m_toplevel_dir + "/master",
        OPEN_FLAG_READ, m_master_file_callback_ptr, &timer);
  }
  catch (Exception &e) {
    if (e.code() != Error::HYPERSPACE_FILE_NOT_FOUND && e.code()
        != Error::HYPERSPACE_BAD_PATHNAME)
      HT_THROW2(e.code(), e, e.what());
  }
  m_hyperspace_init=true;
}

void
MasterClient::create_namespace(const String &name, int flags, DispatchHandler *handler,
                               Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_create_namespace_request(name, flags));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, handler, timer);
}

void
MasterClient::create_namespace(const String &name, int flags, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_create_namespace_request(name, flags));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);

  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event))
    HT_THROWF(MasterProtocol::response_code(event),
              "Master 'create namespace' error, name=%s", name.c_str());

  
}

void
MasterClient::drop_namespace(const String &name, bool if_exists,
                             DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_drop_namespace_request(name, if_exists));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, handler, timer);
}


void
MasterClient::drop_namespace(const String &name, bool if_exists, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_drop_namespace_request(name, if_exists));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event))
    HT_THROWF(MasterProtocol::response_code(event),
              "Master 'drop namespace' error, name=%s", name.c_str());
}

void
MasterClient::create_table(const String &tablename, const String &schema,
                           DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_create_table_request(tablename, schema));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, handler, timer);
}


void
MasterClient::create_table(const String &tablename, const String &schema,
                           Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_create_table_request(tablename, schema));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);

  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event))
    HT_THROWF(MasterProtocol::response_code(event),
              "Master 'create table' error, tablename=%s", tablename.c_str());
}

void
MasterClient::alter_table(const String &tablename, const String &schema,
                          DispatchHandler *handler, Timer *timer) {

  CommBufPtr cbp(MasterProtocol::create_alter_table_request(tablename, schema));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, handler, timer);
}


void
MasterClient::alter_table(const String &tablename, const String &schema,
                          Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_alter_table_request(tablename, schema));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);

  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event))
    HT_THROWF(MasterProtocol::response_code(event),
              "Master 'alter table' error, tablename=%s", tablename.c_str());
}

void
MasterClient::get_schema(const String &tablename, DispatchHandler *handler,
                         Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_get_schema_request(tablename));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, handler, timer);
}


void
MasterClient::get_schema(const String &tablename, std::string &schema,
                         Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_get_schema_request(tablename));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, &sync_handler, timer);
  if (!sync_handler.wait_for_reply(event)) {
    HT_THROWF(MasterProtocol::response_code(event),
              "Master 'get schema' error, tablename=%s", tablename.c_str());
  }
  else {
    const uint8_t *decode_ptr = event->payload + 4;
    size_t decode_remain = event->payload_len - 4;
    schema = decode_vstr(&decode_ptr, &decode_remain);
  }

}


void MasterClient::status(Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_status_request());
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);

  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW(MasterProtocol::response_code(event),
             "Master 'status' error");

}


void
MasterClient::register_server(std::string &location, uint16_t listen_port,
                              StatsSystem &system_stats,
                              DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_register_server_request(location, listen_port, system_stats));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, handler, timer);
}


void MasterClient::register_server(std::string &location, uint16_t listen_port,
                                   StatsSystem &system_stats, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_register_server_request(location, listen_port, system_stats));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);

  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event)) {
    HT_THROW(MasterProtocol::response_code(event),
             "Master 'register server' error");
  }
  else {
    const uint8_t *decode_ptr = event->payload + 4;
    size_t decode_remain = event->payload_len - 4;
    location = decode_vstr(&decode_ptr, &decode_remain);
  }

}


void
MasterClient::move_range(TableIdentifier *table, RangeSpec &range,
                         const String &log_dir, uint64_t soft_limit,
                         bool split, DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_move_range_request(table, range, log_dir,
                                                           soft_limit, split));
  send_message(cbp, handler, timer);
}


void
MasterClient::move_range(TableIdentifier *table, RangeSpec &range,
                         const String &log_dir, uint64_t soft_limit,
                         bool split, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_move_range_request(table, range, log_dir,
                                                           soft_limit, split));
  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event))
    HT_THROWF(MasterProtocol::response_code(event),
              "Master 'move_range' error %s[%s..%s]",
              table->id, range.start_row, range.end_row);

}

void
MasterClient::relinquish_acknowledge(TableIdentifier *table, RangeSpec &range,
                                     DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_relinquish_acknowledge_request(table, range));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, handler, timer);
}


void
MasterClient::relinquish_acknowledge(TableIdentifier *table, RangeSpec &range,
                                     Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_relinquish_acknowledge_request(table, range));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);

  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event))
    HT_THROWF(MasterProtocol::response_code(event),
              "Master 'relinquish_acknowledge' error %s[%s..%s]",
              table->id, range.start_row, range.end_row);

}


void
MasterClient::rename_table(const String &old_name, const String &new_name,
                           DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_rename_table_request(old_name, new_name));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, handler, timer);
}


void
MasterClient::rename_table(const String &old_name, const String &new_name, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_rename_table_request(old_name, new_name));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW(MasterProtocol::response_code(event),
             "Master 'rename table' error");

}
void
MasterClient::drop_table(const String &table_name, bool if_exists,
                         DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_drop_table_request(table_name, if_exists));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, handler, timer);
}


void
MasterClient::drop_table(const String &table_name, bool if_exists, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_drop_table_request(table_name, if_exists));
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW(MasterProtocol::response_code(event),
             "Master 'drop table' error");

}

void MasterClient::close(Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_close_request());
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW(MasterProtocol::response_code(event),
             "Master 'close' error");

}


void MasterClient::shutdown(Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  CommBufPtr cbp(MasterProtocol::create_shutdown_request());
  int64_t id = initiate_operation(cbp, timer);
  cbp = MasterProtocol::create_fetch_result_request(id);
  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW(MasterProtocol::response_code(event),
             "Master 'shutdown' error");

}


void
MasterClient::send_message(CommBufPtr &cbp, DispatchHandler *handler,
                           Timer *timer) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  uint32_t timeout_ms = timer ? timer->remaining() : m_timeout_ms;
  boost::xtime start_time;

  start_time.sec = 0;
  start_time.nsec = 0;

  while ((error = m_comm->send_request(m_master_addr, timeout_ms, cbp, handler))
      != Error::OK) {
    if (error == Error::COMM_NOT_CONNECTED ||
	error == Error::COMM_BROKEN_CONNECTION ||
	error == Error::COMM_CONNECT_ERROR) {
      boost::xtime now;
      int64_t remaining;
      boost::xtime_get(&now, boost::TIME_UTC);
      if (start_time.sec == 0) {
	memcpy(&start_time, &now, sizeof(boost::xtime));
	remaining = timeout_ms;
      }
      else if ((remaining = xtime_diff_millis(start_time, now)) > timeout_ms)
	HT_THROWF(Error::REQUEST_TIMEOUT,
		  "MasterClient send request to %s failed (%s)",
		  m_master_addr.format().c_str(), Error::get_text(error));
      xtime_add_millis(now, 2000);
      m_cond.timed_wait(lock, now);
      continue;
    }
    break;
  }

}

int64_t MasterClient::initiate_operation(CommBufPtr &cbp, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event;
  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event))
    HT_THROW(MasterProtocol::response_code(event),
             "Problem fetching ID for Master operation");  // FIX ME !!!!

  const uint8_t *decode_ptr = event->payload + 4;
  size_t decode_remain = event->payload_len - 4;
  return decode_i64(&decode_ptr, &decode_remain);
}


void MasterClient::reload_master() {
  ScopedLock lock(m_mutex);
  int error;
  DynamicBuffer value(0);
  String addr_str;

  {
    ScopedLock lock(m_hyperspace_mutex);
    if (m_hyperspace_init)
      m_hyperspace->attr_get(m_master_file_handle, "address", value);
    else if (m_hyperspace_connected) {
      initialize_hyperspace();
      m_hyperspace->attr_get(m_master_file_handle, "address", value);
    }
    else
      HT_THROW(Error::CONNECT_ERROR_HYPERSPACE, "MasterClient not connected to Hyperspace");
  }
  addr_str = (const char *)value.base;

  if (addr_str != m_master_addr_string) {

    if (m_master_addr.sin_port != 0) {
      if ((error = m_conn_manager_ptr->remove(m_master_addr)) != Error::OK) {
        if (m_verbose)
          HT_WARNF("Problem removing connection to Master - %s",
                   Error::get_text(error));
      }
      if (m_verbose)
        HT_INFOF("Connecting to new Master (old=%s, new=%s)",
                 m_master_addr_string.c_str(), addr_str.c_str());
    }

    m_master_addr_string = addr_str;

    InetAddr::initialize(&m_master_addr, m_master_addr_string.c_str());

    m_conn_manager_ptr->add(m_master_addr, 15000, "Master",
                            m_dispatcher_handler_ptr);
  }

}


bool MasterClient::wait_for_connection(uint32_t max_wait_ms) {
  Timer timer(max_wait_ms, true);
  m_conn_manager_ptr->wait_for_connection(m_master_addr, timer);
  return m_conn_manager_ptr->get_comm()->wait_for_proxy_load(timer);
}

bool MasterClient::wait_for_connection(Timer &timer) {
  m_conn_manager_ptr->wait_for_connection(m_master_addr, timer);
  return m_conn_manager_ptr->get_comm()->wait_for_proxy_load(timer);
}

void MasterClientHyperspaceSessionCallback::disconnected() {
  m_masterclient->hyperspace_disconnected();
}

void MasterClientHyperspaceSessionCallback::reconnected() {
  m_masterclient->hyperspace_reconnected();
}

