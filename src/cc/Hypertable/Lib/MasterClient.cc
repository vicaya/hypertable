/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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
MasterClient::MasterClient(ConnectionManagerPtr &conn_mgr, Hyperspace::SessionPtr &hyperspace, time_t timeout, ApplicationQueuePtr &app_queue) : m_verbose(true), m_conn_manager_ptr(conn_mgr), m_hyperspace_ptr(hyperspace), m_app_queue_ptr(app_queue), m_timeout(timeout), m_initiated(false) {
  int error;

  m_comm = m_conn_manager_ptr->get_comm();
  memset(&m_master_addr, 0, sizeof(m_master_addr));

  /**
   * Open /hypertable/master Hyperspace file to discover the master.
   */
  m_master_file_callback_ptr = new MasterFileHandler(this, m_app_queue_ptr);
  if ((error = m_hyperspace_ptr->open("/hypertable/master", OPEN_FLAG_READ, m_master_file_callback_ptr, &m_master_file_handle)) != Error::OK) {
    if (error != Error::HYPERSPACE_FILE_NOT_FOUND && error != Error::HYPERSPACE_BAD_PATHNAME) {
      HT_ERRORF("Unable to open Hyperspace file '/hypertable/master' - %s", Error::get_text(error));
      exit(1);
    }
    m_master_file_handle = 0;
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
  return reload_master();
}



int MasterClient::create_table(const char *tablename, const char *schemastr, DispatchHandler *handler) {
  CommBufPtr cbp(MasterProtocol::create_create_table_request(tablename, schemastr));
  return send_message(cbp, handler);
}



int MasterClient::create_table(const char *tablename, const char *schemastr) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_create_table_request(tablename, schemastr));
  int error = send_message(cbp, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'create table' error, tablename=%s : %s", tablename, MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
  }
  return error;
}


int MasterClient::get_schema(const char *tablename, DispatchHandler *handler) {
  CommBufPtr cbp(MasterProtocol::create_get_schema_request(tablename));
  return send_message(cbp, handler);
}


int MasterClient::get_schema(const char *tablename, std::string &schema) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_get_schema_request(tablename));
  int error = send_message(cbp, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'get schema' error, tablename=%s : %s", tablename,
                  MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
    else {
      const uint8_t *ptr = event_ptr->message + sizeof(int32_t);
      size_t remaining = event_ptr->message_len - sizeof(int32_t);
      schema = decode_vstr(&ptr, &remaining);
    }
  }
  return error;
}


int MasterClient::status() {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_status_request());
  int error = send_message(cbp, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'status' error : %s", MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
  }
  return error;
}


int MasterClient::register_server(std::string &location, DispatchHandler *handler) {
  CommBufPtr cbp(MasterProtocol::create_register_server_request(location));
  return send_message(cbp, handler);
}


int MasterClient::register_server(std::string &location) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_register_server_request(location));
  int error = send_message(cbp, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'register server' error : %s", MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
  }
  return error;
}


int MasterClient::report_split(TableIdentifier *table, RangeSpec &range, const char *log_dir, uint64_t soft_limit, DispatchHandler *handler) {
  CommBufPtr cbp(MasterProtocol::create_report_split_request(table, range, log_dir, soft_limit));
  return send_message(cbp, handler);
}


int MasterClient::report_split(TableIdentifier *table, RangeSpec &range, const char *log_dir, uint64_t soft_limit) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_report_split_request(table, range, log_dir, soft_limit));
  int error = send_message(cbp, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'report split' error : %s", MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
  }
  return error;
}



int MasterClient::drop_table(const char *table_name, bool if_exists, DispatchHandler *handler) {
  CommBufPtr cbp(MasterProtocol::create_drop_table_request(table_name, if_exists));
  return send_message(cbp, handler);
}



int MasterClient::drop_table(const char *table_name, bool if_exists) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(MasterProtocol::create_drop_table_request(table_name, if_exists));
  int error = send_message(cbp, &sync_handler);
  if (error == Error::OK) {
    if (!sync_handler.wait_for_reply(event_ptr)) {
      if (m_verbose)
        HT_ERRORF("Master 'drop table' error : %s", MasterProtocol::string_format_message(event_ptr).c_str());
      error = (int)MasterProtocol::response_code(event_ptr);
    }
  }
  return error;
}



int MasterClient::send_message(CommBufPtr &cbp, DispatchHandler *handler) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;

  if ((error = m_comm->send_request(m_master_addr, m_timeout, cbp, handler)) != Error::OK) {
    std::string addr_str;
    if (m_verbose)
      HT_WARNF("Comm::send_request to %s failed - %s", InetAddr::string_format(addr_str, m_master_addr), Error::get_text(error));
  }

  return error;
}


/**
 *
 */
int MasterClient::reload_master() {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  DynamicBuffer value(0);
  std::string addr_str;

  if ((error = m_hyperspace_ptr->attr_get(m_master_file_handle, "address", value)) != Error::OK) {
    if (m_verbose)
      HT_ERRORF("Problem reading 'address' attribute of Hyperspace file /hypertable/master - %s", Error::get_text(error));
    return Error::MASTER_NOT_RUNNING;
  }

  addr_str = (const char *)value.base;

  if (addr_str != m_master_addr_string) {

    if (m_master_addr.sin_port != 0) {
      if ((error = m_conn_manager_ptr->remove(m_master_addr)) != Error::OK) {
        if (m_verbose)
          HT_WARNF("Problem removing connection to Master - %s", Error::get_text(error));
      }
      if (m_verbose)
        HT_INFOF("Connecting to new Master (old=%s, new=%s)", m_master_addr_string.c_str(), addr_str.c_str());
    }

    m_master_addr_string = addr_str;

    InetAddr::initialize(&m_master_addr, m_master_addr_string.c_str());

    m_conn_manager_ptr->add(m_master_addr, 15, "Master", m_dispatcher_handler_ptr);
  }

  return Error::OK;
}


bool MasterClient::wait_for_connection(long max_wait_secs) {
  return m_conn_manager_ptr->wait_for_connection(m_master_addr, max_wait_secs);
}
