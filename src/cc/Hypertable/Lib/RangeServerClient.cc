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
#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/StringExt.h"
#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "RangeServerClient.h"
#include "ScanBlock.h"

using namespace Hypertable;
using namespace Hypertable::Config;


RangeServerClient::RangeServerClient(Comm *comm, uint32_t timeout_ms)
  : m_comm(comm), m_default_timeout_ms(timeout_ms), m_timeout_ms(0) {
  if (timeout_ms == 0)
    m_default_timeout_ms = get_i32("Hypertable.Request.Timeout");
}


RangeServerClient::~RangeServerClient() {
}

void
RangeServerClient::load_range(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const char *transfer_log, const RangeState &range_state,
    DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_load_range(table, range,
                 transfer_log, range_state));
  send_message(addr, cbp, handler);
}

void
RangeServerClient::load_range(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const char *transfer_log, const RangeState &range_state) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(RangeServerProtocol::create_request_load_range(table, range,
                 transfer_log, range_state));
  send_message(addr, cbp, &sync_handler);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW((int)Protocol::response_code(event_ptr),
             String("RangeServer load_range() failure : ")
             + Protocol::string_format_message(event_ptr));
}


void
RangeServerClient::update(const CommAddress &addr, const TableIdentifier &table,
    uint32_t count, StaticBuffer &buffer, uint32_t flags, DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_update(table, count,
                                                            buffer, flags));
  send_message(addr, cbp, handler);
}


void
RangeServerClient::update(const CommAddress &addr, const TableIdentifier &table,
                          uint32_t count, StaticBuffer &buffer, uint32_t flags) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(RangeServerProtocol::create_request_update(table, count,
                                                            buffer, flags));
  send_message(addr, cbp, &sync_handler);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW((int)Protocol::response_code(event_ptr),
             String("RangeServer update() failure : ")
             + Protocol::string_format_message(event_ptr));
}



void
RangeServerClient::create_scanner(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const ScanSpec &scan_spec, DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_create_scanner(table,
                 range, scan_spec));
  send_message(addr, cbp, handler);
}


void
RangeServerClient::create_scanner(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const ScanSpec &scan_spec, ScanBlock &scan_block) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(RangeServerProtocol::create_request_create_scanner(table,
                 range, scan_spec));
  send_message(addr, cbp, &sync_handler);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW((int)Protocol::response_code(event_ptr),
             String("RangeServer create_scanner() failure : ")
             + Protocol::string_format_message(event_ptr));
  else {
    HT_ASSERT(scan_block.load(event_ptr) == Error::OK);
  }
}


void
RangeServerClient::destroy_scanner(const CommAddress &addr, int scanner_id,
                                   DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::
                 create_request_destroy_scanner(scanner_id));
  send_message(addr, cbp, handler);
}


void
RangeServerClient::destroy_scanner(const CommAddress &addr, int scanner_id) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(RangeServerProtocol::
                 create_request_destroy_scanner(scanner_id));
  send_message(addr, cbp, &sync_handler);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW((int)Protocol::response_code(event_ptr),
             String("RangeServer destroy_scanner() failure : ")
             + Protocol::string_format_message(event_ptr));
}


void
RangeServerClient::fetch_scanblock(const CommAddress &addr, int scanner_id,
                                   DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::
                 create_request_fetch_scanblock(scanner_id));
  send_message(addr, cbp, handler);
}


void
RangeServerClient::fetch_scanblock(const CommAddress &addr, int scanner_id,
                                   ScanBlock &scan_block) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(RangeServerProtocol::
                 create_request_fetch_scanblock(scanner_id));
  send_message(addr, cbp, &sync_handler);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW((int)Protocol::response_code(event_ptr),
             String("RangeServer fetch_scanblock() failure : ")
             + Protocol::string_format_message(event_ptr));
  else {
    HT_EXPECT(scan_block.load(event_ptr) == Error::OK,
              Error::FAILED_EXPECTATION);
  }
}


void
RangeServerClient::drop_table(const CommAddress &addr,
    const TableIdentifier &table, DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_drop_table(table));
  send_message(addr, cbp, handler);
}


void
RangeServerClient::drop_table(const CommAddress &addr,
                              const TableIdentifier &table) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(RangeServerProtocol::create_request_drop_table(table));
  send_message(addr, cbp, &sync_handler);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW((int)Protocol::response_code(event_ptr),
             String("RangeServer drop_table() failure : ")
             + Protocol::string_format_message(event_ptr));
}

void
RangeServerClient::update_schema(const CommAddress &addr,
    const TableIdentifier &table, const char *schema,
    DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_update_schema(table,
      schema));
  send_message(addr, cbp, handler);
}

void
RangeServerClient::commit_log_sync(const CommAddress &addr, DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_commit_log_sync());
  send_message(addr, cbp, handler);
}

void RangeServerClient::status(const CommAddress &addr) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(RangeServerProtocol::create_request_status());
  send_message(addr, cbp, &sync_handler);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW((int)Protocol::response_code(event_ptr),
             String("RangeServer status() failure : ")
             + Protocol::string_format_message(event_ptr));
}

void RangeServerClient::close(const CommAddress &addr) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(RangeServerProtocol::create_request_close());
  send_message(addr, cbp, &sync_handler);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW((int)Protocol::response_code(event_ptr),
             String("RangeServer close() failure : ")
             + Protocol::string_format_message(event_ptr));
}


void RangeServerClient::shutdown(const CommAddress &addr) {
  CommBufPtr cbp(RangeServerProtocol::create_request_shutdown());
  send_message(addr, cbp, 0);
}

void RangeServerClient::dump(const CommAddress &addr,
			     String &outfile, bool nokeys) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(RangeServerProtocol::create_request_dump(outfile, nokeys));
  send_message(addr, cbp, &sync_handler);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW((int)Protocol::response_code(event_ptr),
             String("RangeServer dump() failure : ")
             + Protocol::string_format_message(event_ptr));

}

void
RangeServerClient::get_statistics(const CommAddress &addr,
                                  RangeServerStat &stat) {
  DispatchHandlerSynchronizer sync_handler;
  EventPtr event_ptr;
  CommBufPtr cbp(RangeServerProtocol::create_request_get_statistics());
  send_message(addr, cbp, &sync_handler);

  if (!sync_handler.wait_for_reply(event_ptr))
    HT_THROW((int)Protocol::response_code(event_ptr),
             String("RangeServer get_stats() failure : ")
             + Protocol::string_format_message(event_ptr));

  const uint8_t *decode_ptr = event_ptr->payload + 4;
  size_t decode_remain = event_ptr->payload_len - 4;
  stat.decode(&decode_ptr, &decode_remain);
}


void
RangeServerClient::replay_begin(const CommAddress &addr, uint16_t group,
                                DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_begin(group));
  send_message(addr, cbp, handler);
}

void
RangeServerClient::replay_load_range(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    const RangeState &range_state, DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_load_range(table,
                 range, range_state));
  send_message(addr, cbp, handler);
}

void
RangeServerClient::replay_update(const CommAddress &addr, StaticBuffer &buffer,
                                 DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_update(buffer));
  send_message(addr, cbp, handler);
}


void
RangeServerClient::replay_commit(const CommAddress &addr,
                                 DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_replay_commit());
  send_message(addr, cbp, handler);
}


void
RangeServerClient::drop_range(const CommAddress &addr,
    const TableIdentifier &table, const RangeSpec &range,
    DispatchHandler *handler) {
  CommBufPtr cbp(RangeServerProtocol::create_request_drop_range(table, range));
  send_message(addr, cbp, handler);
}


void
RangeServerClient::send_message(const CommAddress &addr, CommBufPtr &cbp,
                                DispatchHandler *handler) {
  int error;
  uint32_t timeout_ms = (m_timeout_ms == 0) ? m_default_timeout_ms
                                            : m_timeout_ms;
  m_timeout_ms = 0;

  if ((error = m_comm->send_request(addr, timeout_ms, cbp, handler))
      != Error::OK) {
    HT_WARNF("Comm::send_request to %s failed - %s",
             addr.to_str().c_str(), Error::get_text(error));
    HT_THROWF(error, "Comm::send_request to %s failed",
              addr.to_str().c_str());
  }
}
