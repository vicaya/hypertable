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
#include "Common/FailureInducer.h"
#include "Common/ScopeGuard.h"
#include "Common/StringExt.h"
#include "Common/md5.h"

#include "AsyncComm/CommAddress.h"

#include "Hyperspace/Session.h"

#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/KeySpec.h"
#include "Hypertable/Lib/RangeServerClient.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/TableMutator.h"
#include "Hypertable/Lib/TableScanner.h"
#include "Hypertable/Lib/Types.h"

#include "Context.h"
#include "Utility.h"

using namespace Hyperspace;

namespace Hypertable { namespace Utility {

void get_table_server_set(ContextPtr &context, const String &id, StringSet &servers) {
  String start_row, end_row;
  ScanSpec scan_spec;
  RowInterval ri;
  TableScannerPtr scanner;
  Cell cell;
  String location;

  start_row = format("%s:", id.c_str());
  end_row = format("%s:%s", id.c_str(), Key::END_ROW_MARKER);

  scan_spec.row_limit = 0;
  scan_spec.max_versions = 1;
  scan_spec.columns.clear();
  scan_spec.columns.push_back("Location");

  ri.start = start_row.c_str();
  ri.end = end_row.c_str();
  scan_spec.row_intervals.push_back(ri);

  scanner = context->metadata_table->create_scanner(scan_spec);

  while (scanner->next(cell)) {
    location = String((const char *)cell.value, cell.value_len);
    boost::trim(location);
    servers.insert(location);
  }
}

bool table_exists(ContextPtr &context, const String &name, String &id) {
  bool is_namespace;

  id = "";

  if (!context->namemap->name_to_id(name, id, &is_namespace) ||
      is_namespace)
    return false;

  String tablefile = context->toplevel_dir + "/tables/" + id;

  try {
    uint64_t handle = 0;
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, context->hyperspace, &handle);
    if (context->hyperspace->exists(tablefile)) {
      handle = context->hyperspace->open(tablefile, OPEN_FLAG_READ);
      if (context->hyperspace->attr_exists(handle, "x"))
        return true;
    }
  }
  catch (Exception &e) {
    if (e.code() == Error::HYPERSPACE_FILE_NOT_FOUND ||
        e.code() == Error::HYPERSPACE_BAD_PATHNAME)
      return false;
    HT_THROW2(e.code(), e, name);
  }
  return false;
}


bool table_exists(ContextPtr &context, const String &id) {

  String tablefile = context->toplevel_dir + "/tables/" + id;

  try {
    uint64_t handle = 0;
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, context->hyperspace, &handle);
    if (context->hyperspace->exists(tablefile)) {
      handle = context->hyperspace->open(tablefile, OPEN_FLAG_READ);
      if (context->hyperspace->attr_exists(handle, "x"))
        return true;
    }
  }
  catch (Exception &e) {
    if (e.code() == Error::HYPERSPACE_FILE_NOT_FOUND ||
        e.code() == Error::HYPERSPACE_BAD_PATHNAME)
      return false;
    HT_THROW2(e.code(), e, id);
  }
  return false;
}


void verify_table_name_availability(ContextPtr &context, const String &name, String &id) {
  bool is_namespace;

  id = "";

  if (!context->namemap->name_to_id(name, id, &is_namespace))
    return;

  if (is_namespace)
    HT_THROW(Error::NAME_ALREADY_IN_USE, "");

  String tablefile = context->toplevel_dir + "/tables/" + id;

  uint64_t handle = 0;
  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, context->hyperspace, &handle);
  if (context->hyperspace->exists(tablefile)) {
    handle = context->hyperspace->open(tablefile, Hyperspace::OPEN_FLAG_READ);
    if (context->hyperspace->attr_exists(handle, "x"))
      HT_THROW(Error::MASTER_TABLE_EXISTS, "");
  }

}


void create_table_in_hyperspace(ContextPtr &context, const String &name,
                                const String &schema_str, TableIdentifierManaged *table) {
  uint64_t handle = 0;
  String table_name = name;

  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, context->hyperspace, &handle);

  // String leading '/'
  if (table_name[0] == '/')
    table_name = table_name.substr(1);

  String table_id;
  Utility::verify_table_name_availability(context, table_name, table_id);

  if (table_id == "")
    context->namemap->add_mapping(table_name, table_id, 0, true);

  HT_MAYBE_FAIL("Utility-create-table-in-hyperspace-1");

  table->set_id(table_id);

  HT_ASSERT(table_name != TableIdentifier::METADATA_NAME ||
            table_id == TableIdentifier::METADATA_ID);

  SchemaPtr schema = Schema::new_instance(schema_str, schema_str.length());
  if (!schema->is_valid())
    HT_THROW(Error::MASTER_BAD_SCHEMA, schema->get_error_string());

  if (schema->need_id_assignment())
    schema->assign_ids();

  table->generation = schema->get_generation();

  String finalschema = "";
  schema->render(finalschema, true);

  // Create table file
  String tablefile = context->toplevel_dir + "/tables/" + table_id;
  int oflags = OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE;
  handle = context->hyperspace->open(tablefile, oflags);

  HT_MAYBE_FAIL("Utility-create-table-in-hyperspace-2");

  // Write schema attribute
  context->hyperspace->attr_set(handle, "schema", finalschema.c_str(),
                         finalschema.length());

  // Create /hypertable/tables/&lt;table&gt;/&lt;accessGroup&gt; directories
  // for this table in DFS
  String table_basedir = context->toplevel_dir + "/tables/" + table_id + "/";

  foreach(const Schema::AccessGroup *ag, schema->get_access_groups()) {
    String agdir = table_basedir + ag->name;
    context->dfs->mkdirs(agdir);
  }

}


void create_table_write_metadata(ContextPtr &context, TableIdentifier *table) {

  if (context->test_mode) {
    HT_WARN("Skipping create_table_write_metadata due to TEST MODE");
    return;
  }

  TableMutatorPtr mutator_ptr = context->metadata_table->create_mutator();

  String metadata_key_str = String(table->id) + ":" + Key::END_ROW_MARKER;
  String start_row;
  KeySpec key;
  key.row = metadata_key_str.c_str();
  key.row_len = metadata_key_str.length();
  key.column_qualifier = 0;
  key.column_qualifier_len = 0;
  key.column_family = "StartRow";

  if (table->is_metadata())
    mutator_ptr->set(key, Key::END_ROOT_ROW, strlen(Key::END_ROOT_ROW));
  else
    mutator_ptr->set(key, 0, 0);

  mutator_ptr->flush();
}

/**
 */

bool next_available_server(ContextPtr &context, String &location) {
  RangeServerConnectionPtr rsc;
  if (!context->next_available_server(rsc))
    return false;
  location = rsc->location();
  return true;
}


void create_table_load_range(ContextPtr &context, const String &location, TableIdentifier *table, RangeSpec &range, bool needs_compaction) {

  try {
    RangeServerClient rsc(context->comm);
    CommAddress addr;
    RangeState range_state;

    addr.set_proxy(location);

    if (table->is_metadata())
      range_state.soft_limit = context->range_split_size;
    else
      range_state.soft_limit = context->range_split_size / std::min(64, (int)context->server_count()*2);

    if (context->test_mode) {
      RangeServerConnectionPtr rsc;
      HT_WARNF("Skipping %s::load_range() because in TEST MODE", location.c_str());
      HT_ASSERT(context->find_server_by_location(location, rsc));
      if (!rsc->connected())
        HT_THROW(Error::COMM_NOT_CONNECTED, "");
    }
    else
      rsc.load_range(addr, *table, range, 0, range_state, needs_compaction);
  }
  catch (Exception &e) {
    if (e.code() != Error::RANGESERVER_RANGE_ALREADY_LOADED)
      throw;
  }
}

int64_t range_hash_code(const TableIdentifier &table, const RangeSpec &range, const char *qualifier) {
  if (qualifier)
    return md5_hash(qualifier) ^ md5_hash(table.id) ^ md5_hash(range.start_row) ^ md5_hash(range.end_row);
  return md5_hash(table.id) ^ md5_hash(range.start_row) ^ md5_hash(range.end_row);
}

String range_hash_string(const TableIdentifier &table, const RangeSpec &range, const char *qualifier) {
  return String("") + range_hash_code(table, range, qualifier);
}


}}
