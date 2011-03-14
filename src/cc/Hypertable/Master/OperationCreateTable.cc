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

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/Key.h"

#include "OperationCreateTable.h"
#include "Utility.h"

#include <boost/algorithm/string.hpp>

using namespace Hypertable;
using namespace Hyperspace;

OperationCreateTable::OperationCreateTable(ContextPtr &context, const String &name, const String &schema)
  : Operation(context, MetaLog::EntityType::OPERATION_CREATE_TABLE), m_name(name), m_schema(schema) {
  initialize_dependencies();
}

OperationCreateTable::OperationCreateTable(ContextPtr &context,
                                           const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
  if (m_table.id && *m_table.id != 0)
    m_range_name = format("%s[..%s]", m_table.id, Key::END_ROW_MARKER);
}

OperationCreateTable::OperationCreateTable(ContextPtr &context, EventPtr &event) 
  : Operation(context, event, MetaLog::EntityType::OPERATION_CREATE_TABLE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  initialize_dependencies();
}

void OperationCreateTable::initialize_dependencies() {
  boost::trim_if(m_name, boost::is_any_of("/ "));
  m_name = String("/") + m_name;
  m_exclusivities.insert(m_name);
  if (!boost::algorithm::starts_with(m_name, "/sys/"))
    m_dependencies.insert(Dependency::INIT);
  m_dependencies.insert(Dependency::METADATA);
  m_dependencies.insert(Dependency::SYSTEM);
}

void OperationCreateTable::execute() {
  bool is_namespace;
  RangeSpec range;
  int32_t state = get_state();

  HT_INFOF("Entering CreateTable-%lld(%s, location=%s) state=%s",
           (Lld)header.id, m_name.c_str(), m_location.c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    // Check to see if namespace exists
    if (m_context->namemap->exists_mapping(m_name, &is_namespace))
      complete_error(Error::NAME_ALREADY_IN_USE, "");
    set_state(OperationState::ASSIGN_ID);
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL("create-table-INITIAL");

  case OperationState::ASSIGN_ID:
    try {
      Utility::create_table_in_hyperspace(m_context, m_name, m_schema, &m_table);
    }
    catch (Exception &e) {
      if (e.code() == Error::INDUCED_FAILURE)
        throw;
      if (e.code() != Error::NAMESPACE_DOES_NOT_EXIST)
        HT_ERROR_OUT << e << HT_END;
      complete_error(e);
      return;
    }
    HT_MAYBE_FAIL("create-table-ASSIGN_ID");
    set_state(OperationState::WRITE_METADATA);

  case OperationState::WRITE_METADATA:
    Utility::create_table_write_metadata(m_context, &m_table);
    HT_MAYBE_FAIL("create-table-WRITE_METADATA-a");
    m_range_name = format("%s[..%s]", m_table.id, Key::END_ROW_MARKER);
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(Dependency::SERVERS);
      m_dependencies.insert(Dependency::METADATA);
      m_dependencies.insert(Dependency::SYSTEM);
      m_dependencies.insert(m_range_name);
      m_state = OperationState::ASSIGN_LOCATION;
    }
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL("create-table-WRITE_METADATA-b");
    return;

  case OperationState::ASSIGN_LOCATION:
    if (!Utility::next_available_server(m_context, m_location))
      return;
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(Dependency::METADATA);
      m_dependencies.insert(Dependency::SYSTEM);
      m_dependencies.insert(m_location);
      m_dependencies.insert(m_range_name);
      m_state = OperationState::LOAD_RANGE;
    }
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL("create-table-ASSIGN_LOCATION");
    return;

  case OperationState::LOAD_RANGE:
    try {
      range.start_row = 0;
      range.end_row = Key::END_ROW_MARKER;
      Utility::create_table_load_range(m_context, m_location, &m_table, range);
      HT_MAYBE_FAIL("create-table-LOAD_RANGE-a");
    }
    catch (Exception &e) {
      if (!m_context->reassigned(&m_table, range, m_location))
        HT_THROW2(e.code(), e, format("Loading %s range %s on server %s",
                                      m_name.c_str(), m_range_name.c_str(), m_location.c_str()));
      // if reassigned, it was properly loaded and then moved, so continue on
    }
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_state = OperationState::FINALIZE;
    }
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL("create-table-LOAD_RANGE-b");

  case OperationState::FINALIZE:
    {
      uint64_t handle = 0;
      HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_context->hyperspace, &handle);
      String tablefile = m_context->toplevel_dir + "/tables/" + m_table.id;
      handle = m_context->hyperspace->open(tablefile, OPEN_FLAG_READ|OPEN_FLAG_WRITE);
      m_context->hyperspace->attr_set(handle, "x", "", 0);
    }
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving CreateTable-%lld(%s, id=%s, generation=%u)",
           (Lld)header.id, m_name.c_str(), m_table.id, (unsigned)m_table.generation);

}


void OperationCreateTable::display_state(std::ostream &os) {
  os << " name=" << m_name << " ";
  if (m_table.id)
    os << m_table << " ";
  os << " location=" << m_location << " ";
}

size_t OperationCreateTable::encoded_state_length() const {
  return Serialization::encoded_length_vstr(m_name) +
    Serialization::encoded_length_vstr(m_schema) +
    m_table.encoded_length() +
    Serialization::encoded_length_vstr(m_location);
}

void OperationCreateTable::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
  Serialization::encode_vstr(bufp, m_schema);
  m_table.encode(bufp);
  Serialization::encode_vstr(bufp, m_location);
}

void OperationCreateTable::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
  m_table.decode(bufp, remainp);
  m_location = Serialization::decode_vstr(bufp, remainp);
}

void OperationCreateTable::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_name = Serialization::decode_vstr(bufp, remainp);
  m_schema = Serialization::decode_vstr(bufp, remainp);
}

const String OperationCreateTable::name() {
  return "OperationCreateTable";
}

const String OperationCreateTable::label() {
  return String("CreateTable ") + m_name;
}

