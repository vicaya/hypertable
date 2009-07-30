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
    Hyperspace::SessionPtr &hyperspace, uint32_t timeout_ms,
    ApplicationQueuePtr &app_queue)
  : m_verbose(true), m_conn_manager_ptr(conn_mgr),
    m_hyperspace(hyperspace), m_app_queue_ptr(app_queue),
    m_hyperspace_init(false), m_hyperspace_connected(true), m_timeout_ms(timeout_ms) {

  m_comm = m_conn_manager_ptr->get_comm();
  memset(&m_master_addr, 0, sizeof(m_master_addr));

  /**
   * Open /hypertable/master Hyperspace file to discover the master.
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
    m_master_file_handle = m_hyperspace->open("/hypertable/master",
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
MasterClient::create_table(const char *tablename, const char *schemastr,
                           DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_create_table_request(tablename,
                                                             schemastr));
  send_message(cbp, handler, timer);
}


void
MasterClient::create_table(const char *tablename, const char *schemastr,
                           Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_create_table_request(tablename,
                                                             schemastr));

  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROWF(MasterProtocol::response_code(event_ptr),
              "Master 'create table' error, tablename=%s", tablename);
}

void
MasterClient::alter_table(const char *tablename, const char *schemastr,
                          DispatchHandler *handler, Timer *timer) {

  CommBufPtr cbp(MasterProtocol::create_alter_table_request(tablename,
      schemastr));
  send_message(cbp, handler, timer);
}


void
MasterClient::alter_table(const char *tablename, const char *schemastr,
                          Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_alter_table_request(tablename,
      schemastr));

  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROWF(MasterProtocol::response_code(event_ptr),
              "Master 'alter table' error, tablename=%s", tablename);
}

void
MasterClient::get_schema(const char *tablename, DispatchHandler *handler,
                         Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_get_schema_request(tablename));
  send_message(cbp, handler, timer);
}


void
MasterClient::get_schema(const char *tablename, std::string &schema,
                         Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_get_schema_request(tablename));
  send_message(cbp, &sync_handler, timer);
  if (!sync_handler.wait_for_reply(event_ptr)) {
    HT_THROWF(MasterProtocol::response_code(event_ptr),
              "Master 'get schema' error, tablename=%s", tablename);
  }
  else {
    const uint8_t *decode_ptr = event_ptr->payload + 4;
    size_t decode_remain = event_ptr->payload_len - 4;
    schema = decode_vstr(&decode_ptr, &decode_remain);
  }

}


void MasterClient::status(Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_status_request());

  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW(MasterProtocol::response_code(event_ptr),
             "Master 'status' error");

}


void
MasterClient::register_server(std::string &location, DispatchHandler *handler,
                              Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_register_server_request(location));
  send_message(cbp, handler, timer);
}


void MasterClient::register_server(std::string &location, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_register_server_request(location));

  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW(MasterProtocol::response_code(event_ptr),
             "Master 'register server' error");

}


void
MasterClient::report_split(TableIdentifier *table, RangeSpec &range,
                           const char *log_dir, uint64_t soft_limit,
                           DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_report_split_request(table, range,
                 log_dir, soft_limit));
  send_message(cbp, handler, timer);
}


void
MasterClient::report_split(TableIdentifier *table, RangeSpec &range,
    const char *log_dir, uint64_t soft_limit, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_report_split_request(table, range,
                 log_dir, soft_limit));

  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROWF(MasterProtocol::response_code(event_ptr),
              "Master 'report split' error %s[%s..%s]",
              table->name, range.start_row, range.end_row);

}


void
MasterClient::drop_table(const char *table_name, bool if_exists,
                         DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_drop_table_request(table_name,
                                                           if_exists));
  send_message(cbp, handler, timer);
}


void
MasterClient::drop_table(const char *table_name, bool if_exists, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_drop_table_request(table_name,
                                                           if_exists));
  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW(MasterProtocol::response_code(event_ptr),
             "Master 'drop table' error");

}

void MasterClient::close(Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_close_request());
  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW(MasterProtocol::response_code(event_ptr),
             "Master 'close' error");

}


void MasterClient::shutdown(Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_shutdown_request());
  send_message(cbp, &sync_handler, timer);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW(MasterProtocol::response_code(event_ptr),
             "Master 'shutdown' error");

}


void
MasterClient::send_message(CommBufPtr &cbp, DispatchHandler *handler,
                           Timer *timer) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  uint32_t timeout_ms = timer ? timer->remaining() : m_timeout_ms;

  if ((error = m_comm->send_request(m_master_addr, timeout_ms, cbp, handler))
      != Error::OK)
    HT_THROWF(error, "MasterClient send request to %s failed",
              m_master_addr.format().c_str());

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
  return m_conn_manager_ptr->wait_for_connection(m_master_addr, max_wait_ms);
}

bool MasterClient::wait_for_connection(Timer &timer) {
  return m_conn_manager_ptr->wait_for_connection(m_master_addr, timer);
}

void MasterClientHyperspaceSessionCallback::disconnected() {
  m_masterclient->hyperspace_disconnected();
}

void MasterClientHyperspaceSessionCallback::reconnected() {
  m_masterclient->hyperspace_reconnected();
}

