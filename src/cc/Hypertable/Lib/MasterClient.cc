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

#include "Common/Error.h"
#include "Common/InetAddr.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Serialization.h"

#include "Hyperspace/Session.h"

#include "MasterFileHandler.h"
#include "MasterClient.h"
#include "MasterProtocol.h"


/**
 * 
 */
MasterClient::MasterClient(ConnectionManagerPtr &connManagerPtr, Hyperspace::SessionPtr &hyperspacePtr, time_t timeout, ApplicationQueuePtr &appQueuePtr) : m_verbose(true), m_conn_manager_ptr(connManagerPtr), m_hyperspace_ptr(hyperspacePtr), m_timeout(timeout), m_app_queue_ptr(appQueuePtr), m_initiated(false) {
  int error;

  m_comm = m_conn_manager_ptr->get_comm();
  memset(&m_master_addr, 0, sizeof(m_master_addr));

  /**
   * Open /hypertable/master Hyperspace file to discover the master.
   */
  m_master_file_callback_ptr = new MasterFileHandler(this, m_app_queue_ptr);
  if ((error = m_hyperspace_ptr->open("/hypertable/master", OPEN_FLAG_READ, m_master_file_callback_ptr, &m_master_file_handle)) != Error::OK) {
    if (error != Error::HYPERSPACE_FILE_NOT_FOUND && error != Error::HYPERSPACE_BAD_PATHNAME) {
      LOG_VA_ERROR("Unable to open Hyperspace file '/hypertable/master' - %s", Error::get_text(error));
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
int MasterClient::initiate_connection(DispatchHandlerPtr dispatchHandlerPtr) {
  m_dispatcher_handler_ptr = dispatchHandlerPtr;
  if (m_master_file_handle == 0)
    return Error::MASTER_NOT_RUNNING;
  return reload_master();
}



int MasterClient::create_table(const char *tableName, const char *schemaString, DispatchHandler *handler) {
  CommBufPtr cbufPtr( MasterProtocol::create_create_table_request(tableName, schemaString) );
  return send_message(cbufPtr, handler);
}



int MasterClient::create_table(const char *tableName, const char *schemaString) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( MasterProtocol::create_create_table_request(tableName, schemaString) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      if (m_verbose)
	LOG_VA_ERROR("Master 'create table' error, tableName=%s : %s", tableName, MasterProtocol::string_format_message(eventPtr).c_str());
      error = (int)MasterProtocol::response_code(eventPtr);
    }
  }
  return error;
}


int MasterClient::get_schema(const char *tableName, DispatchHandler *handler) {
  CommBufPtr cbufPtr( MasterProtocol::create_get_schema_request(tableName) );
  return send_message(cbufPtr, handler);
}


int MasterClient::get_schema(const char *tableName, std::string &schema) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( MasterProtocol::create_get_schema_request(tableName) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      if (m_verbose)
	LOG_VA_ERROR("Master 'get schema' error, tableName=%s : %s", tableName, MasterProtocol::string_format_message(eventPtr).c_str());
      error = (int)MasterProtocol::response_code(eventPtr);
    }
    else {
      uint8_t *ptr = eventPtr->message + sizeof(int32_t);
      size_t remaining = eventPtr->messageLen - sizeof(int32_t);
      const char *schemaStr;
      if (!Serialization::decode_string(&ptr, &remaining, &schemaStr)) {
	if (m_verbose)
	  LOG_VA_ERROR("Problem decoding response to 'get schema' command for table '%s'", tableName);
	return Error::PROTOCOL_ERROR;
      }
      schema = schemaStr;
    }
  }
  return error;
}


int MasterClient::status() {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( MasterProtocol::create_status_request() );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      if (m_verbose)
	LOG_VA_ERROR("Master 'status' error : %s", MasterProtocol::string_format_message(eventPtr).c_str());
      error = (int)MasterProtocol::response_code(eventPtr);
    }
  }
  return error;
}


int MasterClient::register_server(std::string &serverIdStr, DispatchHandler *handler) {
  CommBufPtr cbufPtr( MasterProtocol::create_register_server_request(serverIdStr) );
  return send_message(cbufPtr, handler);
}


int MasterClient::register_server(std::string &serverIdStr) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( MasterProtocol::create_register_server_request(serverIdStr) );
  int error = send_message(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      if (m_verbose)
	LOG_VA_ERROR("Master 'register server' error : %s", MasterProtocol::string_format_message(eventPtr).c_str());
      error = (int)MasterProtocol::response_code(eventPtr);
    }
  }
  return error;
}



int MasterClient::send_message(CommBufPtr &cbufPtr, DispatchHandler *handler) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;

  if ((error = m_comm->send_request(m_master_addr, m_timeout, cbufPtr, handler)) != Error::OK) {
    std::string addrStr;
    if (m_verbose)
      LOG_VA_WARN("Comm::send_request to %s failed - %s", InetAddr::string_format(addrStr, m_master_addr), Error::get_text(error));
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
  std::string addrStr;

  if ((error = m_hyperspace_ptr->attr_get(m_master_file_handle, "address", value)) != Error::OK) {
    if (m_verbose)
      LOG_VA_ERROR("Problem reading 'address' attribute of Hyperspace file /hypertable/master - %s", Error::get_text(error));
    return Error::MASTER_NOT_RUNNING;
  }

  addrStr = (const char *)value.buf;

  if (addrStr != m_master_addr_string) {

    if (m_master_addr.sin_port != 0) {
      if ((error = m_conn_manager_ptr->remove(m_master_addr)) != Error::OK) {
	if (m_verbose)
	  LOG_VA_WARN("Problem removing connection to Master - %s", Error::get_text(error));
      }
      if (m_verbose)
	LOG_VA_INFO("Connecting to new Master (old=%s, new=%s)", m_master_addr_string.c_str(), addrStr.c_str());
    }

    m_master_addr_string = addrStr;

    InetAddr::initialize(&m_master_addr, m_master_addr_string.c_str());

    m_conn_manager_ptr->add(m_master_addr, 15, "Master", m_dispatcher_handler_ptr);
  }

  return Error::OK;
}


bool MasterClient::wait_for_connection(long maxWaitSecs) {
  return m_conn_manager_ptr->wait_for_connection(m_master_addr, maxWaitSecs);
}
