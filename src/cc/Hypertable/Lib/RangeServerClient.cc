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

#include "Common/Error.h"
#include "Common/StringExt.h"
#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "RangeServerClient.h"
#include "ScanBlock.h"

using namespace Hypertable;


RangeServerClient::RangeServerClient(Comm *comm, time_t timeout) : m_comm(comm), m_default_timeout(timeout), m_timeout(0) {
}


RangeServerClient::~RangeServerClient() {
  return;
}

void RangeServerClient::load_range(struct sockaddr_in &addr, TableIdentifier &table, RangeSpec &range, const char *transfer_log, RangeState &range_state, uint16_t flags, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_load_range(table, range, transfer_log, range_state, flags) );
  send_message(addr, cbufPtr, handler);
}

void RangeServerClient::load_range(struct sockaddr_in &addr, TableIdentifier &table, RangeSpec &range, const char *transfer_log, RangeState &range_state, uint16_t flags) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr event_ptr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_load_range(table, range, transfer_log, range_state, flags) );
  send_message(addr, cbufPtr, &syncHandler);
  if (!syncHandler.wait_for_reply(event_ptr))
    throw Exception((int)Protocol::response_code(event_ptr),
		    std::string("RangeServer load_range() failure : ") + Protocol::string_format_message(event_ptr));
}


void RangeServerClient::update(struct sockaddr_in &addr, TableIdentifier &table, uint8_t *data, size_t len, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_update(table, data, len) );
  send_message(addr, cbufPtr, handler);
}


void RangeServerClient::update(struct sockaddr_in &addr, TableIdentifier &table, uint8_t *data, size_t len) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr event_ptr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_update(table, data, len) );
  send_message(addr, cbufPtr, &syncHandler);
  if (!syncHandler.wait_for_reply(event_ptr))
    throw Exception((int)Protocol::response_code(event_ptr),
		    std::string("RangeServer update() failure : ") + Protocol::string_format_message(event_ptr));
}



void RangeServerClient::create_scanner(struct sockaddr_in &addr, TableIdentifier &table, RangeSpec &range, ScanSpec &scan_spec, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_create_scanner(table, range, scan_spec) );
  send_message(addr, cbufPtr, handler);
}


void RangeServerClient::create_scanner(struct sockaddr_in &addr, TableIdentifier &table, RangeSpec &range, ScanSpec &scan_spec, ScanBlock &scan_block) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr event_ptr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_create_scanner(table, range, scan_spec) );
  send_message(addr, cbufPtr, &syncHandler);
  if (!syncHandler.wait_for_reply(event_ptr))
    throw Exception((int)Protocol::response_code(event_ptr),
		    std::string("RangeServer create_scanner() failure : ") + Protocol::string_format_message(event_ptr));
  else {
    HT_EXPECT(scan_block.load(event_ptr) == Error::OK, Error::FAILED_EXPECTATION);
  }
}


void RangeServerClient::destroy_scanner(struct sockaddr_in &addr, int scanner_id, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_destroy_scanner(scanner_id) );
  send_message(addr, cbufPtr, handler);
}


void RangeServerClient::destroy_scanner(struct sockaddr_in &addr, int scanner_id) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr event_ptr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_destroy_scanner(scanner_id) );
  send_message(addr, cbufPtr, &syncHandler);
  if (!syncHandler.wait_for_reply(event_ptr))
    throw Exception((int)Protocol::response_code(event_ptr),
		    std::string("RangeServer destroy_scanner() failure : ") + Protocol::string_format_message(event_ptr));
}


void RangeServerClient::fetch_scanblock(struct sockaddr_in &addr, int scanner_id, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_fetch_scanblock(scanner_id) );
  send_message(addr, cbufPtr, handler);
}


void RangeServerClient::fetch_scanblock(struct sockaddr_in &addr, int scanner_id, ScanBlock &scan_block) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr event_ptr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_fetch_scanblock(scanner_id) );
  send_message(addr, cbufPtr, &syncHandler);
  if (!syncHandler.wait_for_reply(event_ptr))
    throw Exception((int)Protocol::response_code(event_ptr),
		    std::string("RangeServer fetch_scanblock() failure : ") + Protocol::string_format_message(event_ptr));
  else {
    HT_EXPECT(scan_block.load(event_ptr) == Error::OK, Error::FAILED_EXPECTATION);
  }
}


void RangeServerClient::drop_table(struct sockaddr_in &addr, TableIdentifier &table, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_drop_table(table) );
  send_message(addr, cbufPtr, handler);
}


void RangeServerClient::drop_table(struct sockaddr_in &addr, TableIdentifier &table) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr event_ptr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_drop_table(table) );
  send_message(addr, cbufPtr, &syncHandler);
  if (!syncHandler.wait_for_reply(event_ptr))
    throw Exception((int)Protocol::response_code(event_ptr),
		    std::string("RangeServer drop_table() failure : ") + Protocol::string_format_message(event_ptr));
}


void RangeServerClient::status(struct sockaddr_in &addr) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr event_ptr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_status() );
  send_message(addr, cbufPtr, &syncHandler);
  if (!syncHandler.wait_for_reply(event_ptr))
    throw Exception((int)Protocol::response_code(event_ptr),
		    std::string("RangeServer status() failure : ") + Protocol::string_format_message(event_ptr));
}

void RangeServerClient::shutdown(struct sockaddr_in &addr) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr event_ptr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_shutdown() );
  send_message(addr, cbufPtr, &syncHandler);
  if (!syncHandler.wait_for_reply(event_ptr))
    throw Exception((int)Protocol::response_code(event_ptr),
		    std::string("RangeServer shutdown() failure : ") + Protocol::string_format_message(event_ptr));
}

void RangeServerClient::dump_stats(struct sockaddr_in &addr) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr event_ptr;
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_dump_stats() );
  send_message(addr, cbufPtr, &syncHandler);
  if (!syncHandler.wait_for_reply(event_ptr))
    throw Exception((int)Protocol::response_code(event_ptr),
		    std::string("RangeServer dump_stats() failure : ") + Protocol::string_format_message(event_ptr));
}


void RangeServerClient::replay_start(struct sockaddr_in &addr, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_replay_start() );
  send_message(addr, cbufPtr, handler);
}


void RangeServerClient::replay_update(struct sockaddr_in &addr, const uint8_t *data, size_t len, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_replay_update(data, len) );
  send_message(addr, cbufPtr, handler);
}


void RangeServerClient::replay_commit(struct sockaddr_in &addr, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_replay_commit() );
  send_message(addr, cbufPtr, handler);
}


void RangeServerClient::drop_range(struct sockaddr_in &addr, TableIdentifier &table, RangeSpec &range, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::create_request_drop_range(table, range) );
  send_message(addr, cbufPtr, handler);
}



/**
 *
 */
void RangeServerClient::send_message(struct sockaddr_in &addr, CommBufPtr &cbufPtr, DispatchHandler *handler) {
  int error;
  time_t timeout = (m_timeout == 0) ? m_default_timeout : m_timeout;

  m_timeout = 0;
  if ((error = m_comm->send_request(addr, timeout, cbufPtr, handler)) != Error::OK) {
    HT_WARNF("Comm::send_request to %s:%d failed - %s",
		inet_ntoa(addr.sin_addr), ntohs(addr.sin_port), Error::get_text(error));
    throw Exception(error, std::string("Comm::send_request to ") + inet_ntoa(addr.sin_addr) + ":" + ntohs(addr.sin_port) + " failed");
  }
}
