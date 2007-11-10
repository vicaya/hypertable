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

using namespace hypertable;


RangeServerClient::RangeServerClient(Comm *comm, time_t timeout) : m_comm(comm), m_timeout(timeout) {
}


RangeServerClient::~RangeServerClient() {
  return;
}


int RangeServerClient::load_range(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_load_range(addr, rangeSpec) );
  return send_message(addr, cbufPtr, handler);
}

int RangeServerClient::load_range(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_load_range(addr, rangeSpec) );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      LOG_VA_ERROR("RangeServer 'load range' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
  }
  return error;
}


int RangeServerClient::update(struct sockaddr_in &addr, std::string tableName, uint32_t generation, uint8_t *data, size_t len, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_update(addr, tableName, generation, data, len) );
  return send_message(addr, cbufPtr, handler);
}


int RangeServerClient::update(struct sockaddr_in &addr, std::string tableName, uint32_t generation, uint8_t *data, size_t len) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_update(addr, tableName, generation, data, len) );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      LOG_VA_ERROR("RangeServer 'update' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
  }
  return error;
}


int RangeServerClient::create_scanner(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, ScanSpecificationT &scanSpec, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_create_scanner(addr, rangeSpec, scanSpec) );
  return send_message(addr, cbufPtr, handler);
}


int RangeServerClient::create_scanner(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, ScanSpecificationT &scanSpec, ScanBlock &scanblock) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_create_scanner(addr, rangeSpec, scanSpec) );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      LOG_VA_ERROR("RangeServer 'create scanner' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
    else
      error = scanblock.load(eventPtr);
  }
  return error;
}


int RangeServerClient::fetch_scanblock(struct sockaddr_in &addr, int scannerId, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_fetch_scanblock(addr, scannerId) );
  return send_message(addr, cbufPtr, handler);
}


int RangeServerClient::fetch_scanblock(struct sockaddr_in &addr, int scannerId, ScanBlock &scanblock) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_fetch_scanblock(addr, scannerId) );
  int error = send_message(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.wait_for_reply(eventPtr)) {
      LOG_VA_ERROR("RangeServer 'fetch scanblock' error : %s", Protocol::string_format_message(eventPtr).c_str());
      error = (int)Protocol::response_code(eventPtr);
    }
    else
      error = scanblock.load(eventPtr);
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
      LOG_VA_ERROR("RangeServer 'status' error : %s", Protocol::string_format_message(eventPtr).c_str());
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
    LOG_VA_WARN("Comm::send_request to %s:%d failed - %s",
		inet_ntoa(addr.sin_addr), ntohs(addr.sin_port), Error::get_text(error));
  }
  return error;
}
