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
#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "RangeServerClient.h"
#include "ScanBlock.h"

using namespace Hypertable;


RangeServerClient::RangeServerClient(Comm *comm, time_t timeout) : m_comm(comm), m_timeout(timeout) {
}


RangeServerClient::~RangeServerClient() {
  return;
}

int RangeServerClient::load_range(struct sockaddr_in &addr, TableIdentifierT &table, RangeT &range, uint64_t soft_limit, uint16_t flags, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_load_range(table, range, soft_limit, flags) );
  return send_message(addr, cbufPtr, handler);
}

int RangeServerClient::load_range(struct sockaddr_in &addr, TableIdentifierT &table, RangeT &range, uint64_t soft_limit, uint16_t flags) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_load_range(table, range, soft_limit, flags) );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("RangeServer 'load range' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
  }
  return error;
}


int RangeServerClient::update(struct sockaddr_in &addr, TableIdentifierT &table, uint8_t *data, size_t len, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_update(table, data, len) );
  return send_message(addr, cbufPtr, handler);
}


int RangeServerClient::update(struct sockaddr_in &addr, TableIdentifierT &table, uint8_t *data, size_t len) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_update(table, data, len) );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("RangeServer 'update' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
  }
  return error;
}



int RangeServerClient::create_scanner(struct sockaddr_in &addr, TableIdentifierT &table, RangeT &range, ScanSpecificationT &scan_spec, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_create_scanner(table, range, scan_spec) );
  return send_message(addr, cbufPtr, handler);
}


int RangeServerClient::create_scanner(struct sockaddr_in &addr, TableIdentifierT &table, RangeT &range, ScanSpecificationT &scan_spec, ScanBlock &scan_block) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_create_scanner(table, range, scan_spec) );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("RangeServer 'create scanner' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
    else
      error = scan_block.load(eventPtr);
  }
  return error;
}


int RangeServerClient::destroy_scanner(struct sockaddr_in &addr, int scanner_id, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_destroy_scanner(scanner_id) );
  return send_message(addr, cbufPtr, handler);
}


int RangeServerClient::destroy_scanner(struct sockaddr_in &addr, int scanner_id) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_destroy_scanner(scanner_id) );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("RangeServer 'fetch scanblock' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
  }
  return error;
}


int RangeServerClient::fetch_scanblock(struct sockaddr_in &addr, int scanner_id, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_fetch_scanblock(scanner_id) );
  return send_message(addr, cbufPtr, handler);
}


int RangeServerClient::fetch_scanblock(struct sockaddr_in &addr, int scanner_id, ScanBlock &scan_block) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_fetch_scanblock(scanner_id) );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("RangeServer 'fetch scanblock' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
    else
      error = scan_block.load(eventPtr);
  }
  return error;
}


int RangeServerClient::drop_table(struct sockaddr_in &addr, std::string table_name, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_drop_table(table_name) );
  return send_message(addr, cbufPtr, handler);
}


int RangeServerClient::drop_table(struct sockaddr_in &addr, std::string table_name) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_drop_table(table_name) );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("RangeServer 'drop table' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
  }
  return error;
}


int RangeServerClient::status(struct sockaddr_in &addr) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_status() );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("RangeServer 'status' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
  }
  return error;
}

int RangeServerClient::shutdown(struct sockaddr_in &addr) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_shutdown() );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("RangeServer 'shutdown' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
  }
  return error;
}

int RangeServerClient::dump_stats(struct sockaddr_in &addr) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_dump_stats() );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("RangeServer 'status' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
  }
  return error;
}


/**
 *
 */
int RangeServerClient::send_message(struct sockaddr_in &addr, CommBufPtr &cbufPtr, DispatchHandler *handler) {
  int error;

  if ((error = m_comm->send_request(addr, m_timeout, cbufPtr, handler)) != Error::OK) {
    HT_WARNF("Comm::send_request to %s:%d failed - %s",
		inet_ntoa(addr.sin_addr), ntohs(addr.sin_port), Error::get_text(error));
  }
  return error;
}
