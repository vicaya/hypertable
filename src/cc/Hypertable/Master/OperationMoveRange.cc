/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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
#include "Common/FailureInducer.h"
#include "Common/ScopeGuard.h"
#include "Common/Serialization.h"
#include "Common/System.h"
#include "Common/md5.h"

#include "AsyncComm/ResponseCallback.h"

#include "Hypertable/Lib/Key.h"

#include "OperationMoveRange.h"
#include "OperationProcessor.h"
#include "Utility.h"

using namespace Hypertable;
using namespace Hyperspace;

OperationMoveRange::OperationMoveRange(ContextPtr &context, const TableIdentifier &table,
                                       const RangeSpec &range, const String &transfer_log,
                                       uint64_t soft_limit, bool is_split)
  : Operation(context, MetaLog::EntityType::OPERATION_MOVE_RANGE),
    m_table(table), m_range(range), m_transfer_log(transfer_log),
    m_soft_limit(soft_limit), m_is_split(is_split), m_remove_explicitly(true) {
  m_range_name = format("%s[%s..%s]", m_table.id, m_range.start_row, m_range.end_row);
  initialize_dependencies();
}

OperationMoveRange::OperationMoveRange(ContextPtr &context,
                                       const MetaLog::EntityHeader &header_)
  : Operation(context, header_), m_remove_explicitly(true) {
}

OperationMoveRange::OperationMoveRange(ContextPtr &context, EventPtr &event)
  : Operation(context, event, MetaLog::EntityType::OPERATION_MOVE_RANGE), m_remove_explicitly(true) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  initialize_dependencies();
}

void OperationMoveRange::initialize_dependencies() {
  m_exclusivities.insert(Utility::range_hash_string(m_table, m_range, "OperationMoveRange"));
  m_dependencies.insert(Dependency::INIT);
  m_dependencies.insert(Dependency::SERVERS);
  m_dependencies.insert(Utility::range_hash_string(m_table, m_range));
  if (!strcmp(m_table.id, TableIdentifier::METADATA_ID)) {
    if (*m_range.start_row == 0 && !strcmp(m_range.end_row, Key::END_ROOT_ROW))
      m_obstructions.insert(Dependency::ROOT);
    else {
      m_obstructions.insert(Dependency::METADATA);
      m_dependencies.insert(Dependency::ROOT);
    }
  }
  else if (!strncmp(m_table.id, "0/", 2)) {
    m_obstructions.insert(Dependency::SYSTEM);
    m_dependencies.insert(Dependency::ROOT);
    m_dependencies.insert(Dependency::METADATA);
  }
  else {
    m_dependencies.insert(Dependency::ROOT);
    m_dependencies.insert(Dependency::METADATA);
    m_dependencies.insert(Dependency::SYSTEM);
  }
  m_obstructions.insert(String("id:") + m_table.id);
}


void OperationMoveRange::execute() {
  int32_t state = get_state();

  HT_INFOF("Entering MoveRange-%lld %s state=%s",
           (Lld)header.id, m_range_name.c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    // Check to see if this operation was already executed
    if (m_context->in_progress(this) ||
        m_context->response_manager->operation_complete(hash_code())) {
      HT_INFOF("Aborting %s because already in progress", label().c_str());
      abort_operation();
      return;
    }
    if (!Utility::next_available_server(m_context, m_location))
      return;
    {
      ScopedLock lock(m_mutex);
      m_dependencies.insert(m_location);
      m_state = OperationState::STARTED;
    }
    if (!m_context->add_in_progress(this)) {
      HT_INFOF("Aborting %s because already in progress", label().c_str());
      abort_operation();
      return;
    }
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL("move-range-INITIAL");

    return;

  case OperationState::STARTED:
    if (m_event) {
      try {
        ResponseCallback cb(m_context->comm, m_event);
        cb.response_ok();
      }
      catch (Exception &e) {
        HT_WARN_OUT << e << HT_END;
      }
    }
    m_event = 0;
    set_state(OperationState::LOAD_RANGE);

  case OperationState::LOAD_RANGE:
    try {
      RangeServerClient rsc(m_context->comm);
      CommAddress addr;
      RangeState range_state;
      TableIdentifier *table = &m_table;
      RangeSpec *range = &m_range;

      addr.set_proxy(m_location);
      range_state.soft_limit = m_soft_limit;
      if (m_context->test_mode)
        HT_WARNF("Skipping %s::load_range() because in TEST MODE", m_location.c_str());
      else
        rsc.load_range(addr, *table, *range, m_transfer_log.c_str(), range_state, false);
    }
    catch (Exception &e) {
      if (e.code() != Error::RANGESERVER_RANGE_ALREADY_LOADED &&
          e.code() != Error::RANGESERVER_TABLE_DROPPED &&
          !m_context->reassigned(&m_table, m_range, m_location)) {
        if (!Utility::table_exists(m_context, m_table.id)) {
          HT_WARNF("Aborting MoveRange %s because table no longer exists",
                   m_range_name.c_str());
          complete_ok();
          return;
        }
        if (!m_context->is_connected(m_location))
          return;
        HT_THROW2(e.code(), e, format("MoveRange %s to %s", m_range_name.c_str(), m_location.c_str()));
      }
    }
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving MoveRange-%lld %s -> %s",
           (Lld)header.id, m_range_name.c_str(), m_location.c_str());
}


void OperationMoveRange::display_state(std::ostream &os) {
  os << " " << m_table << " " << m_range << " transfer-log='" << m_transfer_log;
  os << "' soft-limit=" << m_soft_limit << " is_split=" << ((m_is_split) ? "true" : "false");
  os << " location='" << m_location << " ";
}

size_t OperationMoveRange::encoded_state_length() const {
  return m_table.encoded_length() + m_range.encoded_length() +
    Serialization::encoded_length_vstr(m_transfer_log) + 9 +
    Serialization::encoded_length_vstr(m_location);
}

void OperationMoveRange::encode_state(uint8_t **bufp) const {
  m_table.encode(bufp);
  m_range.encode(bufp);
  Serialization::encode_vstr(bufp, m_transfer_log);
  Serialization::encode_i64(bufp, m_soft_limit);
  Serialization::encode_bool(bufp, m_is_split);
  Serialization::encode_vstr(bufp, m_location);
}

void OperationMoveRange::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
  m_location = Serialization::decode_vstr(bufp, remainp);
  int32_t state = get_state();
  if (state != OperationState::INITIAL && state != OperationState::COMPLETE)
    HT_ASSERT(m_context->add_in_progress(this));
}

void OperationMoveRange::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_table.decode(bufp, remainp);
  m_range.decode(bufp, remainp);
  m_transfer_log = Serialization::decode_vstr(bufp, remainp);
  m_soft_limit = Serialization::decode_i64(bufp, remainp);
  m_is_split = Serialization::decode_bool(bufp, remainp);
  m_range_name = format("%s[%s..%s]", m_table.id, m_range.start_row, m_range.end_row);
}

int64_t OperationMoveRange::hash_code() const {
  return Utility::range_hash_code(m_table, m_range, "OperationMoveRange");
}

const String OperationMoveRange::name() {
  return "OperationMoveRange";
}

const String OperationMoveRange::label() {
  return format("MoveRange %s[%s..%s]", m_table.id, m_range.start_row, m_range.end_row);
}

const String OperationMoveRange::graphviz_label() {
  String start_row = m_range.start_row;
  String end_row = m_range.end_row;

  if (start_row.length() > 20)
    start_row = start_row.substr(0, 10) + ".." + start_row.substr(start_row.length()-10, 10);

  if (!strcmp(end_row.c_str(), Key::END_ROW_MARKER))
    end_row = "END_ROW_MARKER";
  else if (end_row.length() > 20)
    end_row = end_row.substr(0, 10) + ".." + end_row.substr(end_row.length()-10, 10);

  return format("MoveRange %s\\n%s\\n%s", m_table.id, start_row.c_str(), end_row.c_str());
}

void OperationMoveRange::abort_operation() {
  ScopedLock lock(m_mutex);
  m_state = OperationState::COMPLETE;
  m_remove_explicitly = false;
  m_expiration_time.reset();
  int32_t error;
  ResponseCallback cb(m_context->comm, m_event);
  if ((error = cb.response_ok()) != Error::OK) {
    HT_INFOF("Problem sending OK response for %s", m_range_name.c_str());
    return;
  }
}
