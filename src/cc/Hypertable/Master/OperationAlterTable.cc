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

#include "DispatchHandlerOperationAlterTable.h"
#include "OperationAlterTable.h"
#include "Utility.h"

#include <boost/algorithm/string.hpp>

using namespace Hypertable;
using namespace Hyperspace;

OperationAlterTable::OperationAlterTable(ContextPtr &context, const String &name, const String &schema)
  : Operation(context, MetaLog::EntityType::OPERATION_ALTER_TABLE), m_name(name), m_schema(schema) {
  initialize_dependencies();
}

OperationAlterTable::OperationAlterTable(ContextPtr &context,
                                         const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}

OperationAlterTable::OperationAlterTable(ContextPtr &context, EventPtr &event) 
  : Operation(context, event, MetaLog::EntityType::OPERATION_ALTER_TABLE) {
  const uint8_t *ptr = event->payload;
  size_t remaining = event->payload_len;
  decode_request(&ptr, &remaining);
  initialize_dependencies();
}

void OperationAlterTable::initialize_dependencies() {
  boost::trim_if(m_name, boost::is_any_of("/ "));
  m_name = String("/") + m_name;
  m_exclusivities.insert(m_name);
  add_dependency(Dependency::INIT);
  add_dependency(Dependency::METADATA);
  add_dependency(Dependency::SYSTEM);
}

void OperationAlterTable::execute() {
  String filename;
  bool is_namespace;
  StringSet servers;
  DispatchHandlerOperationPtr op_handler;
  TableIdentifier table;
  int32_t state = get_state();
  DependencySet dependencies;

  HT_INFOF("Entering AlterTable-%lld(%s) state=%s",
           (Lld)header.id, m_name.c_str(), OperationState::get_text(state));

  switch (state) {

  case OperationState::INITIAL:
    // Check to see if namespace exists
    if(m_context->namemap->name_to_id(m_name, m_id, &is_namespace)) {
      if (is_namespace) {
        response_error(Error::TABLE_NOT_FOUND, format("%s is a namespace", m_name.c_str()));
        return;
      }
      set_state(OperationState::VALIDATE_SCHEMA);
      m_context->mml_writer->record_state(this);
    }
    else {
      response_error(Error::TABLE_NOT_FOUND, m_name);
      return;
    }
    HT_MAYBE_FAIL("alter-table-INITIAL");

  case OperationState::VALIDATE_SCHEMA:
    try {
      SchemaPtr alter_schema;
      SchemaPtr existing_schema;
      DynamicBuffer value_buf;
      uint64_t handle = 0;
      String filename;

      HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_context->hyperspace, &handle);

      alter_schema = Schema::new_instance(m_schema, m_schema.length());
      if (!alter_schema->is_valid())
        HT_THROW(Error::MASTER_BAD_SCHEMA, alter_schema->get_error_string());
      if (alter_schema->need_id_assignment())
        HT_THROW(Error::MASTER_BAD_SCHEMA, "Updated schema needs ID assignment");

      filename = m_context->toplevel_dir + "/tables/" + m_id;

      handle = m_context->hyperspace->open(filename,
                   OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_LOCK_EXCLUSIVE);

      m_context->hyperspace->attr_get(handle, "schema", value_buf);
      existing_schema = Schema::new_instance((char *)value_buf.base,
                                             strlen((char *)value_buf.base));
      value_buf.clear();

      uint32_t generation =  existing_schema->get_generation()+1;
      if (alter_schema->get_generation() != generation) {
        HT_THROW(Error::MASTER_SCHEMA_GENERATION_MISMATCH,
                 (String) "Expected updated schema generation " + generation
                 + " got " + alter_schema->get_generation());
      }
    }
    catch (Exception &e) {
      if (e.code() != Error::MASTER_BAD_SCHEMA &&
          e.code() != Error::MASTER_SCHEMA_GENERATION_MISMATCH)
        HT_ERROR_OUT << e << HT_END;
      response_error(e);
      return;
    }
    set_state(OperationState::SCAN_METADATA);

  case OperationState::SCAN_METADATA:  // Mabye ditch this state???
    servers.clear();
    Utility::get_table_server_set(m_context, m_id, servers);
    {
      ScopedLock lock(m_mutex);
      m_dependencies.clear();
      m_dependencies.insert(Dependency::INIT);
      m_dependencies.insert(Dependency::METADATA);
      m_dependencies.insert(Dependency::SYSTEM);
      for (StringSet::iterator iter=servers.begin(); iter!=servers.end(); ++iter) {
        if (m_completed.count(*iter) == 0)
          m_dependencies.insert(*iter);
      }
      m_state = OperationState::ISSUE_REQUESTS;
    }
    m_context->mml_writer->record_state(this);
    return;

  case OperationState::ISSUE_REQUESTS:
    table.id = m_id.c_str();
    table.generation = 0;
    {
      ScopedLock lock(m_mutex);
      dependencies = m_dependencies;
    }
    dependencies.erase(Dependency::INIT);
    dependencies.erase(Dependency::METADATA);
    dependencies.erase(Dependency::SYSTEM);
    op_handler = new DispatchHandlerOperationAlterTable(m_context, table, m_schema);
    op_handler->start(dependencies);
    if (!op_handler->wait_for_completion()) {
      std::vector<DispatchHandlerOperation::Result> results;
      op_handler->get_results(results);
      for (size_t i=0; i<results.size(); i++) {
        if (results[i].error == Error::OK ||
            results[i].error == Error::TABLE_NOT_FOUND ||
            results[i].error == Error::RANGESERVER_TABLE_DROPPED) {
          ScopedLock lock(m_mutex);
          m_completed.insert(results[i].location);
          m_dependencies.erase(results[i].location);
        }
        else
          HT_WARNF("Alter table error at %s - %s (%s)", results[i].location.c_str(),
                   Error::get_text(results[i].error), results[i].msg.c_str());
      }
      set_state(OperationState::SCAN_METADATA);
      m_context->mml_writer->record_state(this);
      return;
    }
    set_state(OperationState::UPDATE_HYPERSPACE);
    m_context->mml_writer->record_state(this);

  case OperationState::UPDATE_HYPERSPACE:
    {
      uint64_t handle = 0;
      String filename = m_context->toplevel_dir + "/tables/" + m_id;

      HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_context->hyperspace, &handle);

      handle = m_context->hyperspace->open(filename,
                   OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_LOCK_EXCLUSIVE);
      m_context->hyperspace->attr_set(handle, "schema", m_schema.c_str(),
                                      m_schema.length());
    }
    response_ok();
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving AlterTable-%lld(%s)", (Lld)header.id, m_name.c_str());

}


void OperationAlterTable::display_state(std::ostream &os) {
  os << " name=" << m_name << " id=" << m_id << " ";
}

size_t OperationAlterTable::encoded_state_length() const {
  size_t length = Serialization::encoded_length_vstr(m_name) +
    Serialization::encoded_length_vstr(m_schema) +
    Serialization::encoded_length_vstr(m_id) + 4;
  for (StringSet::iterator iter = m_completed.begin(); iter != m_completed.end(); ++iter)
    length += Serialization::encoded_length_vstr(*iter);
  return length;
}

void OperationAlterTable::encode_state(uint8_t **bufp) const {
  Serialization::encode_vstr(bufp, m_name);
  Serialization::encode_vstr(bufp, m_schema);
  Serialization::encode_vstr(bufp, m_id);
  Serialization::encode_i32(bufp, m_completed.size());
  for (StringSet::iterator iter = m_completed.begin(); iter != m_completed.end(); ++iter)
    Serialization::encode_vstr(bufp, *iter);
}

void OperationAlterTable::decode_state(const uint8_t **bufp, size_t *remainp) {
  decode_request(bufp, remainp);
  m_id = Serialization::decode_vstr(bufp, remainp);
  size_t length = Serialization::decode_i32(bufp, remainp);
  for (size_t i=0; i<length; i++)
    m_completed.insert( Serialization::decode_vstr(bufp, remainp) );
}

void OperationAlterTable::decode_request(const uint8_t **bufp, size_t *remainp) {
  m_name = Serialization::decode_vstr(bufp, remainp);
  m_schema = Serialization::decode_vstr(bufp, remainp);
}

const String OperationAlterTable::name() {
  return "OperationAlterTable";
}

const String OperationAlterTable::label() {
  return String("AlterTable ") + m_name;
}

