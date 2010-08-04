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
#include <cassert>
#include <cstdlib>

extern "C" {
#include <poll.h>
}

#include <boost/algorithm/string.hpp>

#include "AsyncComm/Comm.h"
#include "AsyncComm/ReactorFactory.h"

#include "Common/Init.h"
#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/ScopeGuard.h"
#include "Common/System.h"
#include "Common/Timer.h"

#include "Hyperspace/DirEntry.h"
#include "Hypertable/Lib/Config.h"

#include "Client.h"
#include "HqlCommandInterpreter.h"

using namespace std;
using namespace Hypertable;
using namespace Hyperspace;
using namespace Config;

#define HT_CLIENT_REQ_BEGIN \
  do { \
    try

#define HT_CLIENT_REQ_END \
    catch (Exception &e) { \
      if (!m_refresh_schema || (e.code() != Error::RANGESERVER_GENERATION_MISMATCH && \
          e.code() != Error::MASTER_SCHEMA_GENERATION_MISMATCH)) \
        throw; \
      HT_WARNF("%s - %s", Error::get_text(e.code()), e.what()); \
      refresh_table(table_name); /* name must be defined in calling context */\
      continue; \
    } \
    break; \
  } while (true)



Client::Client(const String &install_dir, const String &config_file,
               uint32_t default_timeout_ms)
  : m_timeout_ms(default_timeout_ms), m_install_dir(install_dir) {
  ScopedRecLock lock(rec_mutex);

  if (!properties)
    init_with_policy<DefaultCommPolicy>(0, 0);

  m_props = new Properties(config_file, file_desc());
  initialize();
  m_namemap = new NameIdMapper(m_hyperspace, m_toplevel_dir);
}


Client::Client(const String &install_dir, uint32_t default_timeout_ms)
  : m_timeout_ms(default_timeout_ms), m_install_dir(install_dir) {
  ScopedRecLock lock(rec_mutex);

  if (!properties)
    init_with_policy<DefaultCommPolicy>(0, 0);

  if (m_install_dir.empty())
    m_install_dir = System::install_dir;

  m_props = properties;
  initialize();
  m_namemap = new NameIdMapper(m_hyperspace, m_toplevel_dir);
}


void Client::create_table(const String &table_name, const String &schema) {
  m_master_client->create_table(table_name, schema);
}


void Client::alter_table(const String &table_name, const String &alter_schema_str) {
  // Construct a new schema which is a merge of the existing schema
  // and the desired alterations.
  SchemaPtr schema, alter_schema, final_schema;
  Schema::AccessGroup *final_ag;
  Schema::ColumnFamily *final_cf;
  String final_schema_str;
  TablePtr table = open_table(table_name);

  HT_CLIENT_REQ_BEGIN {
    schema = table->schema();
    alter_schema = Schema::new_instance(alter_schema_str, alter_schema_str.length());
    if (!alter_schema->is_valid())
      HT_THROW(Error::BAD_SCHEMA, alter_schema->get_error_string());

    final_schema = new Schema(*(schema.get()));
    final_schema->incr_generation();

    foreach(Schema::AccessGroup *alter_ag, alter_schema->get_access_groups()) {
      // create a new access group if needed
      if(!final_schema->access_group_exists(alter_ag->name)) {
        final_ag = new Schema::AccessGroup();
        final_ag->name = alter_ag->name;
        final_ag->in_memory = alter_ag->in_memory;
        final_ag->blocksize = alter_ag->blocksize;
        final_ag->compressor = alter_ag->compressor;
        final_ag->bloom_filter = alter_ag->bloom_filter;
        if (!final_schema->add_access_group(final_ag)) {
          String error_msg = final_schema->get_error_string();
          delete final_ag;
          HT_THROW(Error::BAD_SCHEMA, error_msg);
        }
      }
      else {
        final_ag = final_schema->get_access_group(alter_ag->name);
      }
    }

    // go through each column family to be altered
    foreach(Schema::ColumnFamily *alter_cf, alter_schema->get_column_families()) {
      if (alter_cf->deleted) {
        if (!final_schema->drop_column_family(alter_cf->name))
          HT_THROW(Error::BAD_SCHEMA, final_schema->get_error_string());
      }
      else if (alter_cf->renamed) {
        if (!final_schema->rename_column_family(alter_cf->name, alter_cf->new_name))
          HT_THROW(Error::BAD_SCHEMA, final_schema->get_error_string());
      }
      else {
        // add column family
        if(final_schema->get_max_column_family_id() >= Schema::ms_max_column_id)
          HT_THROW(Error::TOO_MANY_COLUMNS, (String)"Attempting to add > "
                   + Schema::ms_max_column_id
                   + (String) " column families to table");
        final_schema->incr_max_column_family_id();
        final_cf = new Schema::ColumnFamily(*alter_cf);
        final_cf->id = (uint32_t) final_schema->get_max_column_family_id();
        final_cf->generation = final_schema->get_generation();

        if(!final_schema->add_column_family(final_cf)) {
          String error_msg = final_schema->get_error_string();
          delete final_cf;
          HT_THROW(Error::BAD_SCHEMA, error_msg);
        }
      }
    }

    final_schema->render(final_schema_str, true);
    m_master_client->alter_table(table_name, final_schema_str);
  }
  HT_CLIENT_REQ_END;
}


Table *Client::open_table(const String &table_name, bool force) {

  {
    ScopedLock lock(m_mutex);
    TableCache::iterator it = m_table_cache.find(table_name);

    if (it != m_table_cache.end()) {
      if (force || it->second->need_refresh())
        it->second->refresh();

      return it->second.get();
    }
  }
  Table *table = new Table(m_props, m_range_locator, m_conn_manager,
                           m_hyperspace, m_app_queue, table_name, m_timeout_ms);
  {
    ScopedLock lock(m_mutex);
    TableCache::iterator it = m_table_cache.find(table_name);

    if (it != m_table_cache.end()) {
      delete table;
      return it->second.get();
    }
    m_table_cache.insert(make_pair(table_name, table));
  }
  return table;
}


void Client::refresh_table(const String &table_name) {
  open_table(table_name, true);
}

bool Client::exists_table(const String &table_name) {
  String table_id;
  bool is_namespace = false;
  
  if (!m_namemap->name_to_id(table_name, table_id, &is_namespace) ||
      is_namespace)
    return false;

  // TODO: issue 11

  String table_file = m_toplevel_dir + "/tables/" + table_id;

  DynamicBuffer value_buf(0);
  Hyperspace::HandleCallbackPtr null_handle_callback;
  uint64_t handle = 0;

  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle);

  try {
    handle = m_hyperspace->open(table_file, OPEN_FLAG_READ, null_handle_callback);
    if (!m_hyperspace->attr_exists(handle, "x"))
      return false;
  }
  catch(Exception &e) {
    if (e.code() == Error::HYPERSPACE_FILE_NOT_FOUND ||
        e.code() == Error::HYPERSPACE_BAD_PATHNAME)
      return false;
    HT_THROW(e.code(), e.what());
  }
  return true;
}


String Client::get_table_id(const String &table_name) {
  String table_id;
  bool is_namespace;

  if (!m_namemap->name_to_id(table_name, table_id, &is_namespace) ||
      is_namespace)
    HT_THROW(Error::TABLE_NOT_FOUND, table_name);
  return table_id;
}

String Client::get_schema_str(const String &table_name, bool with_ids) {
  String schema;

  refresh_table(table_name);

  {
    ScopedLock lock(m_mutex);
    TableCache::iterator it = m_table_cache.find(table_name);
    if (it != m_table_cache.end()) {
      it->second->schema()->render(schema, with_ids);
    }
  }

  return schema;
}

SchemaPtr Client::get_schema(const String &table_name) {
  SchemaPtr schema;

  refresh_table(table_name);

  {
    String schema_str;
    ScopedLock lock(m_mutex);
    TableCache::iterator it = m_table_cache.find(table_name);
    HT_ASSERT(it != m_table_cache.end());
    schema = new Schema(*(it->second->schema()));
  }
  return schema;
}

void Client::get_tables(std::vector<String> &tables) {
  uint64_t handle;
  HandleCallbackPtr null_handle_callback;
  std::vector<Hyperspace::DirEntry> listing;
  String display_name;
  bool is_namespace;

  handle = m_hyperspace->open(m_toplevel_dir + "/tables", OPEN_FLAG_READ,
                              null_handle_callback);

  m_hyperspace->readdir(handle, listing);

  for (size_t i=0; i<listing.size(); i++) {
    if (!listing[i].is_dir) {
      HT_ASSERT(m_namemap->id_to_name(listing[i].name, display_name, &is_namespace) && !is_namespace);
      tables.push_back(display_name);
    }
  }

  m_hyperspace->close(handle);
}


void Client::drop_table(const String &table_name, bool if_exists) {

  {
    ScopedLock lock(m_mutex);

    // remove it from cache
    TableCache::iterator it = m_table_cache.find(table_name);

    if (it != m_table_cache.end()) {
      // Put it in the graveyard, another client thread may still hold a reference to it
      m_table_graveyard.push_back(it->second);
      m_table_cache.erase(it);
    }
  }
  HT_CLIENT_REQ_BEGIN {
    m_master_client->drop_table(table_name, if_exists);
  }
  HT_CLIENT_REQ_END;
}


void Client::get_table_splits(const String &name, TableSplitsContainer &splits) {
  TablePtr table;
  TableIdentifierManaged tid;
  SchemaPtr schema;
  char start_row[16];
  char end_row[16];
  TableScannerPtr scanner_ptr;
  ScanSpec scan_spec;
  Cell cell;
  String str;
  Hypertable::RowInterval ri;
  String last_row;
  TableSplitBuilder tsbuilder(splits.arena());
  ProxyMapT proxy_map;

  table = open_table(name);

  table->get(tid, schema);

  table = open_table("METADATA");

  sprintf(start_row, "%s:", tid.id);
  sprintf(end_row, "%s:%s", tid.id, Key::END_ROW_MARKER);

  scan_spec.row_limit = 0;
  scan_spec.max_versions = 1;
  scan_spec.columns.clear();
  scan_spec.columns.push_back("Location");
  scan_spec.columns.push_back("StartRow");

  ri.start = start_row;
  ri.end = end_row;
  scan_spec.row_intervals.push_back(ri);

  scanner_ptr = table->create_scanner(scan_spec);

  m_comm->get_proxy_map(proxy_map);

  while (scanner_ptr->next(cell)) {
    if (strcmp(last_row.c_str(), cell.row_key) && last_row != "") {
      const char *ptr = strchr(last_row.c_str(), ':');
      HT_ASSERT(ptr);
      tsbuilder.set_end_row(ptr+1);
      splits.push_back(tsbuilder.get());
      tsbuilder.clear();
    }
    if (!strcmp(cell.column_family, "Location")) {
      str = String((const char *)cell.value, cell.value_len);
      boost::trim(str);
      tsbuilder.set_location(str);
      ProxyMapT::iterator pmiter = proxy_map.find(str);
      if (pmiter != proxy_map.end())
	tsbuilder.set_ip_address( (*pmiter).second.format_ipaddress() );
    }
    else if (!strcmp(cell.column_family, "StartRow")) {
      str = String((const char *)cell.value, cell.value_len);
      boost::trim(str);
      tsbuilder.set_start_row(str);
    }
    else
      HT_FATALF("Unexpected column family - %s", cell.column_family);
    last_row = cell.row_key;
  }

  tsbuilder.set_end_row(Key::END_ROW_MARKER);
  splits.push_back(tsbuilder.get());


}


Hyperspace::SessionPtr& Client::get_hyperspace_session()
{
  return m_hyperspace;
}

void Client::close() {
  m_master_client->close();
}

void Client::shutdown() {
  m_master_client->shutdown();
}


HqlInterpreter *Client::create_hql_interpreter() {
  return new HqlInterpreter(this, this->m_conn_manager);
}

// ------------- PRIVATE METHODS -----------------
void Client::initialize() {
  uint32_t wait_time, remaining;
  uint32_t interval=5000;

  m_toplevel_dir = m_props->get_str("Hypertable.Directory");
  boost::trim_if(m_toplevel_dir, boost::is_any_of("/"));
  m_toplevel_dir = String("/") + m_toplevel_dir;

  m_comm = Comm::instance();
  m_conn_manager = new ConnectionManager(m_comm);

  if (m_timeout_ms == 0)
    m_timeout_ms = m_props->get_i32("Hypertable.Request.Timeout");

  m_hyperspace_reconnect = m_props->get_bool("Hyperspace.Session.Reconnect");

  m_hyperspace = new Hyperspace::Session(m_comm, m_props);

  m_refresh_schema = m_props->get_bool("Hypertable.Client.RefreshSchema");

  Timer timer(m_timeout_ms, true);

  remaining = timer.remaining();
  wait_time = (remaining < interval) ? remaining : interval;

  while (!m_hyperspace->wait_for_connection(wait_time)) {

    if (timer.expired())
      HT_THROW_(Error::CONNECT_ERROR_HYPERSPACE);

    cout << "Waiting for connection to Hyperspace..." << endl;

    remaining = timer.remaining();
    wait_time = (remaining < interval) ? remaining : interval;
  }

  m_app_queue = new ApplicationQueue(m_props->
                                     get_i32("Hypertable.Client.Workers"));
  m_master_client = new MasterClient(m_conn_manager, m_hyperspace, m_toplevel_dir,
                                     m_timeout_ms, m_app_queue);

  m_master_client->initiate_connection(0);

  if (!m_master_client->wait_for_connection(timer))
    HT_THROW(Error::REQUEST_TIMEOUT, "Waiting for Master connection");

  m_range_locator = new RangeLocator(m_props, m_conn_manager, m_hyperspace,
                                     m_timeout_ms);
}
