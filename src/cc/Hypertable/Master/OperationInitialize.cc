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

#include "Hypertable/Lib/Key.h"

#include "OperationCreateNamespace.h"
#include "OperationCreateTable.h"
#include "OperationInitialize.h"
#include "Utility.h"

using namespace Hypertable;
using namespace Hyperspace;

OperationInitialize::OperationInitialize(ContextPtr &context)
  : Operation(context, MetaLog::EntityType::OPERATION_INITIALIZE) {
  m_obstructions.insert(Dependency::INIT);
}

OperationInitialize::OperationInitialize(ContextPtr &context,
                                         const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}


void OperationInitialize::execute() {
  std::vector<Entity *> entities;
  Operation *operation = 0;
  String filename, schema;
  uint64_t handle = 0;
  RangeSpec range;
  int32_t state = get_state();

  HT_INFOF("Entering Initialize-%lld state=%s",
           (Lld)header.id, OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:

    // Initialize Hyperspace DIRECTORIES and FILES
    {
      handle = 0;
      HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_context->hyperspace, &handle);
      if (!m_context->hyperspace->exists(m_context->toplevel_dir))
        m_context->hyperspace->mkdirs(m_context->toplevel_dir);
      if (!m_context->hyperspace->exists(m_context->toplevel_dir + "/servers"))
        m_context->hyperspace->mkdir(m_context->toplevel_dir + "/servers");
      if (!m_context->hyperspace->exists(m_context->toplevel_dir + "/tables"))
        m_context->hyperspace->mkdir(m_context->toplevel_dir + "/tables");
      handle = m_context->hyperspace->open(m_context->toplevel_dir + "/master",
                                           OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE);
      m_context->hyperspace->close(handle);
      handle = m_context->hyperspace->open(m_context->toplevel_dir + "/root",
                                  OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE);
    }

    {
      ScopedLock lock(m_mutex);
      m_state = OperationState::STARTED;
      m_dependencies.insert("initialize[0]");
    }
    operation = new OperationCreateNamespace(m_context, "/sys", 0);
    operation->add_obstruction("initialize[0]");
    entities.push_back(operation);
    entities.push_back(this);
    m_context->mml_writer->record_state(entities);
    {
      ScopedLock lock(m_mutex);
      m_sub_ops.push_back(operation);
    }
    HT_MAYBE_FAIL("initialize-INITIAL");
    return;

  case OperationState::STARTED:
    filename = System::install_dir + "/conf/METADATA.xml";
    schema = FileUtils::file_to_string(filename);
    Utility::create_table_in_hyperspace(m_context, "/sys/METADATA", schema, &m_table);
    HT_MAYBE_FAIL("initialize-STARTED");
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(Dependency::SERVERS);
      m_state = OperationState::ASSIGN_METADATA_RANGES;
    }
    m_context->mml_writer->record_state(this);
    m_root_range_name = format("%s[%s..%s]", m_table.id, "", Key::END_ROOT_ROW);
    m_metadata_range_name = format("%s[%s..%s]", m_table.id, Key::END_ROOT_ROW, Key::END_ROW_MARKER);

  case OperationState::ASSIGN_METADATA_RANGES:
    if (!Utility::next_available_server(m_context, m_metadata_root_location) ||
        !Utility::next_available_server(m_context, m_metadata_secondlevel_location))
      return;
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(m_metadata_root_location);
      m_dependencies.insert(m_metadata_secondlevel_location);
      m_dependencies.insert( m_root_range_name );
      m_dependencies.insert( m_metadata_range_name );
      m_state = OperationState::LOAD_ROOT_METADATA_RANGE;
    }
    m_context->mml_writer->record_state(this);
    HT_MAYBE_FAIL("initialize-ASSIGN_METADATA_RANGES");

  case OperationState::LOAD_ROOT_METADATA_RANGE:
    try {
      range.start_row = 0;
      range.end_row = Key::END_ROOT_ROW;
      Utility::create_table_load_range(m_context, m_metadata_root_location, &m_table, range,
          false);
      HT_MAYBE_FAIL("initialize-LOAD_ROOT_METADATA_RANGE");
    }
    catch (Exception &e) {
      if (!m_context->reassigned(&m_table, range, m_metadata_root_location))
        HT_THROW2(e.code(), e, format("Loading ROOT range (%s)", m_root_range_name.c_str()));
      // if reassigned, it was properly loaded and then moved, so continue on
    }
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(m_metadata_secondlevel_location);
      m_dependencies.insert( m_metadata_range_name );
      m_state = OperationState::LOAD_SECOND_METADATA_RANGE;
    }
    m_context->mml_writer->record_state(this);

  case OperationState::LOAD_SECOND_METADATA_RANGE:
    try {
      range.start_row = Key::END_ROOT_ROW;
      range.end_row = Key::END_ROW_MARKER;
      Utility::create_table_load_range(m_context, m_metadata_secondlevel_location, &m_table,
          range, false);
      HT_MAYBE_FAIL("initialize-LOAD_SECOND_METADATA_RANGE");
    }
    catch (Exception &e) {
      if (!m_context->reassigned(&m_table, range, m_metadata_secondlevel_location))
        HT_THROW2(e.code(), e, format("Loading second METADATA range (%s)", m_metadata_range_name.c_str()));
      // if reassigned, it was properly loaded and then moved, so continue on
    }
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_state = OperationState::WRITE_METADATA;
    }
    m_context->mml_writer->record_state(this);

  case OperationState::WRITE_METADATA:
    // Write METADATA entry for 2nd level METADATA range
    m_context->metadata_table = new Table(m_context->props, m_context->conn_manager,
                                          m_context->hyperspace, m_context->namemap,
                                          TableIdentifier::METADATA_NAME);
    Utility::create_table_write_metadata(m_context, &m_table);
    {
      uint64_t handle = 0;
      HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_context->hyperspace, &handle);
      String tablefile = m_context->toplevel_dir + "/tables/" + m_table.id;
      handle = m_context->hyperspace->open(tablefile, OPEN_FLAG_READ|OPEN_FLAG_WRITE);
      m_context->hyperspace->attr_set(handle, "x", "", 0);
    }
    HT_MAYBE_FAIL("initialize-WRITE_METADATA");
    {
      m_dependencies.clear();
      m_state = OperationState::CREATE_RS_METRICS;
    }
    m_context->mml_writer->record_state(this);
    return;

  case OperationState::CREATE_RS_METRICS:
    if (!m_context->metadata_table)
      m_context->metadata_table = new Table(m_context->props, m_context->conn_manager,
                                            m_context->hyperspace, m_context->namemap,
                                            TableIdentifier::METADATA_NAME);
    filename = System::install_dir + "/conf/RS_METRICS.xml";
    schema = FileUtils::file_to_string(filename);
    operation = new OperationCreateTable(m_context, "/sys/RS_METRICS", schema);
    operation->add_obstruction("initialize[1]");
    {
      ScopedLock lock(m_mutex);
      m_dependencies.insert("initialize[1]");
      m_state = OperationState::FINALIZE;
      m_sub_ops.push_back(operation);
    }
    entities.clear();
    entities.push_back(operation);
    entities.push_back(this);
    m_context->mml_writer->record_state(entities);
    HT_MAYBE_FAIL("initialize-CREATE_RS_METRICS");
    return;

  case OperationState::FINALIZE:
    if (!m_context->metadata_table)
      m_context->metadata_table = new Table(m_context->props, m_context->conn_manager,
                                            m_context->hyperspace, m_context->namemap,
                                            TableIdentifier::METADATA_NAME);
    complete_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving Initialize-%lld", (Lld)header.id);

}


void OperationInitialize::display_state(std::ostream &os) {
  os << " root-location='" << m_metadata_root_location;
  os << "' 2nd-metadata-location='" << m_metadata_secondlevel_location << "' ";
  os << m_table << " ";
}

size_t OperationInitialize::encoded_state_length() const {
  return Serialization::encoded_length_vstr(m_metadata_root_location) +
    Serialization::encoded_length_vstr(m_metadata_secondlevel_location) +
    m_table.encoded_length();
}

void OperationInitialize::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_metadata_root_location);
  Serialization::encode_vstr(bufp, m_metadata_secondlevel_location);
  m_table.encode(bufp);
}

void OperationInitialize::decode_state(const uint8_t **bufp, size_t *remainp) {
  m_metadata_root_location = Serialization::decode_vstr(bufp, remainp);
  m_metadata_secondlevel_location = Serialization::decode_vstr(bufp, remainp);
  m_table.decode(bufp, remainp);
  if (m_table.id && *m_table.id != 0) {
    m_root_range_name = format("%s[%s..%s]", m_table.id, "", Key::END_ROOT_ROW);
    m_metadata_range_name = format("%s[%s..%s]", m_table.id, Key::END_ROOT_ROW, Key::END_ROW_MARKER);
  }
}

void OperationInitialize::decode_result(const uint8_t **bufp, size_t *remainp) {
  // We need to do this here because we don't know the
  // state until we're decoding and if the state is COMPLETE,
  // this method is called instead of decode_state
  if (is_complete())
    m_context->metadata_table = new Table(m_context->props, m_context->conn_manager,
                                          m_context->hyperspace, m_context->namemap,
                                          TableIdentifier::METADATA_NAME);
  Operation::decode_result(bufp, remainp);
}


const String OperationInitialize::name() {
  return "OperationInitialize";
}

const String OperationInitialize::label() {
  return String("Initialize");
}

