/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include "Hyperspace/Session.h"

#include "MasterFileHandler.h"
#include "MasterClient.h"
#include "MasterProtocol.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
MasterClient::MasterClient(ConnectionManagerPtr &conn_mgr,
    Hyperspace::SessionPtr &hyperspace, uint32_t timeout_millis,
    ApplicationQueuePtr &app_queue)
  : m_verbose(true), m_conn_manager_ptr(conn_mgr),
    m_hyperspace_ptr(hyperspace), m_app_queue_ptr(app_queue),
    m_timeout_millis(timeout_millis), m_initiated(false) {

  m_comm = m_conn_manager_ptr->get_comm();
  memset(&m_master_addr, 0, sizeof(m_master_addr));

  /**
   * Open /hypertable/master Hyperspace file to discover the master.
   */
  m_master_file_callback_ptr = new MasterFileHandler(this, m_app_queue_ptr);
  m_master_file_handle = 0;
  try {
    m_master_file_handle = m_hyperspace_ptr->open("/hypertable/master",
        OPEN_FLAG_READ, m_master_file_callback_ptr);
  }
  catch (Exception &e) {
    if (e.code() != Error::HYPERSPACE_FILE_NOT_FOUND && e.code()
        != Error::HYPERSPACE_BAD_PATHNAME)
      HT_THROW2(e.code(), e, e.what());
  }
}



MasterClient::~MasterClient() {
  m_hyperspace_ptr->close(m_master_file_handle);
}



/**
 *
 */
int MasterClient::initiate_connection(DispatchHandlerPtr dhp) {
  m_dispatcher_handler_ptr = dhp;
  if (m_master_file_handle == 0)
    return Error::MASTER_NOT_RUNNING;
  try {
    reload_master();
  }
  catch (Exception &e) {
    return e.code();
  }
  return Error::OK;
}



int MasterClient::create_table(const char *tablename, const char *schemastr, DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_create_table_request(tablename, schemastr));
  return send_message(cbp, handler, timer);
}



int MasterClient::create_table(const char *tablename, const char *schemastr, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_create_table_request(tablename, schemastr));
  int error = send_message(cbp, &sync_handler, timer);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'create table' error, tablename=%s : %s", tablename,
                  MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
  }
  return error;
}


int MasterClient::get_schema(const char *tablename, DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_get_schema_request(tablename));
  return send_message(cbp, handler, timer);
}


int MasterClient::get_schema(const char *tablename, std::string &schema, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_get_schema_request(tablename));
  int error = send_message(cbp, &sync_handler, timer);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'get schema' error, tablename=%s : %s", tablename,
                  MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
    else {
      const uint8_t *decode_ptr = event_ptr->payload + 4;
      size_t decode_remain = event_ptr->payload_len - 4;
      schema = decode_vstr(&decode_ptr, &decode_remain);
    }
  }
  return error;
}


int MasterClient::status(Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_status_request());
  int error = send_message(cbp, &sync_handler, timer);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'status' error : %s",
                  MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
  }
  return error;
}


int MasterClient::register_server(std::string &location, DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_register_server_request(location));
  return send_message(cbp, handler, timer);
}


int MasterClient::register_server(std::string &location, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_register_server_request(location));
  int error = send_message(cbp, &sync_handler, timer);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'register server' error : %s",
                  MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
  }
  return error;
}


int MasterClient::report_split(TableIdentifier *table, RangeSpec &range, const char *log_dir, uint64_t soft_limit, DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_report_split_request(table, range, log_dir, soft_limit));
  return send_message(cbp, handler, timer);
}


int MasterClient::report_split(TableIdentifier *table, RangeSpec &range, const char *log_dir, uint64_t soft_limit, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_report_split_request(table, range, log_dir, soft_limit));
  int error = send_message(cbp, &sync_handler, timer);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'report split' error : %s",
                  MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
  }
  return error;
}


int MasterClient::drop_table(const char *table_name, bool if_exists, DispatchHandler *handler, Timer *timer) {
  CommBufPtr cbp(MasterProtocol::create_drop_table_request(table_name, if_exists));
  return send_message(cbp, handler, timer);
}



int MasterClient::drop_table(const char *table_name, bool if_exists, Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_drop_table_request(table_name, if_exists));
  int error = send_message(cbp, &sync_handler, timer);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'drop table' error : %s",
                  MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
  }
  return error;
}

int MasterClient::shutdown(Timer *timer) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_shutdown_request());
  int error = send_message(cbp, &sync_handler, timer);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'shutdown' error : %s",
                  MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
  }
  return error;
}




int MasterClient::send_message(CommBufPtr &cbp, DispatchHandler *handler, Timer *timer) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  uint32_t timeout_millis = timer ? timer->remaining() : m_timeout_millis;

  if ((error = m_comm->send_request(m_master_addr, timeout_millis, cbp, handler)) != Error::OK) {
    std::string addr_str;
    if (m_verbose)
      HT_WARNF("Comm::send_request to %s failed - %s",
               InetAddr::format(m_master_addr).c_str(), Error::get_text(error));
  }

  return error;
}


/**
 *
 */
void MasterClient::reload_master() {
  ScopedLock lock(m_mutex);
  int error;
  DynamicBuffer value(0);
  String addr_str;

  m_hyperspace_ptr->attr_get(m_master_file_handle, "address", value);

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


bool MasterClient::wait_for_connection(uint32_t max_wait_millis) {
  return m_conn_manager_ptr->wait_for_connection(m_master_addr, max_wait_millis);
}

bool MasterClient::wait_for_connection(Timer &timer) {
  return m_conn_manager_ptr->wait_for_connection(m_master_addr, timer);
}
