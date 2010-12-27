/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#include <algorithm>
#include <cstdlib>

#include <iostream>
#include <fstream>

#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>

#include "Common/FileUtils.h"
#include "Common/InetAddr.h"
#include "Common/ScopeGuard.h"
#include "Common/StringExt.h"
#include "Common/SystemInfo.h"

#include "DfsBroker/Lib/Client.h"
#include "Hypertable/Lib/LocationCache.h"
#include "Hypertable/Lib/RangeServerClient.h"
#include "Hypertable/Lib/RangeState.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/Lib/Client.h"
#include "Hyperspace/DirEntry.h"
#include "Hyperspace/Session.h"

#include "DispatchHandlerDropTable.h"
#include "DispatchHandlerUpdateSchema.h"
#include "DispatchHandlerGetStatistics.h"

#include "Master.h"
#include "ServersDirectoryHandler.h"
#include "ServerLockFileHandler.h"
#include "RangeServerState.h"

using namespace Hyperspace;
using namespace Hypertable;
using namespace Hypertable::DfsBroker;
using namespace std;

namespace Hypertable {


Master::Master(PropertiesPtr &props, ConnectionManagerPtr &conn_mgr,
               ApplicationQueuePtr &app_queue)
  : m_props_ptr(props), m_conn_manager_ptr(conn_mgr),
    m_app_queue_ptr(app_queue), m_verbose(false), m_dfs_client(0),
    m_next_server_id(1), m_initialized(false), m_root_server_connected(false), m_get_stats_outstanding(false) {

  m_server_map_iter = m_server_map.begin();

  m_toplevel_dir = props->get_str("Hypertable.Directory");
  boost::trim_if(m_toplevel_dir, boost::is_any_of("/"));
  m_toplevel_dir = String("/") + m_toplevel_dir;

  m_hyperspace_ptr = new Hyperspace::Session(conn_mgr->get_comm(), props);
  m_hyperspace_ptr->add_callback(&m_hyperspace_session_handler);
  uint32_t timeout = props->get_i32("Hyperspace.Timeout");

  if (!m_hyperspace_ptr->wait_for_connection(timeout)) {
    HT_ERROR("Unable to connect to hyperspace, exiting...");
    exit(1);
  }
  m_namemap = new NameIdMapper(m_hyperspace_ptr, m_toplevel_dir);

  m_verbose = props->get_bool("Hypertable.Verbose");
  uint16_t port = props->get_i16("Hypertable.Master.Port");
  m_max_range_bytes = props->get_i64("Hypertable.RangeServer.Range.SplitSize");

  m_maintenance_interval = props->get_i32("Hypertable.Monitoring.Interval");

  /**
   * Create DFS Client connection
   */
  DfsBroker::Client *dfs_client = new DfsBroker::Client(conn_mgr, props);

  int dfs_timeout;
  if (props->has("DfsBroker.Timeout"))
    dfs_timeout = props->get_i32("DfsBroker.Timeout");
  else
    dfs_timeout = props->get_i32("Hypertable.Request.Timeout");

  if (!dfs_client->wait_for_connection(dfs_timeout)) {
    HT_ERROR("Unable to connect to DFS Broker, exiting...");
    exit(1);
  }
  m_dfs_client = dfs_client;

  m_rangeserver_port = props->get_i16("Hypertable.RangeServer.Port");

  if (!initialize())
    exit(1);

  m_monitoring = new Monitoring(props);

  /* Read Last Table ID */
  {
    DynamicBuffer valbuf(0);
    HandleCallbackPtr null_handle_callback;
    int ival;
    uint32_t lock_status;
    uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;

    m_master_file_handle = m_hyperspace_ptr->open(m_toplevel_dir + "/master", oflags,
                                                  null_handle_callback);

    m_hyperspace_ptr->try_lock(m_master_file_handle, LOCK_MODE_EXCLUSIVE,
                               &lock_status, &m_master_file_sequencer);

    if (lock_status != LOCK_STATUS_GRANTED) {
      HT_ERRORF("Unable to obtain lock on '%s/master' - conflict", m_toplevel_dir.c_str());
      exit(1);
    }

    // Write master location in 'address' attribute, format is IP:port
    InetAddr addr(System::net_info().primary_addr, port);
    String addr_s = addr.format();
    m_hyperspace_ptr->attr_set(m_master_file_handle, "address",
                               addr_s.c_str(), addr_s.length());

    try {
      m_hyperspace_ptr->attr_get(m_master_file_handle, "next_server_id", valbuf);
      ival = atoi((const char *)valbuf.base);
    }
    catch (Exception &e) {
      if (e.code() == Error::HYPERSPACE_ATTR_NOT_FOUND) {
        m_hyperspace_ptr->attr_set(m_master_file_handle, "next_server_id", "1", 2);
        ival = 1;
      }
      else
        HT_THROW2(e.code(), e, e.what());
    }
    m_next_server_id = (uint32_t)ival;
  }

  /**
   * Locate tablet servers
   */
  scan_servers_directory();

  master_gc_start(props, m_threads, m_metadata_table_ptr, m_dfs_client);


}

Master::~Master() {
  delete m_dfs_client;
}



/**
 *
 */
void Master::server_joined(const String &location) {
  HT_INFOF("Server Joined (%s)", location.c_str());
  cout << flush;
}



/**
 *
 */
void Master::server_left(const String &location) {
  LockSequencer lock_sequencer;
  String hsfname = m_toplevel_dir + "/servers/" + location;
  InetAddr connection;
  bool was_connected = false;

  HT_INFOF("Server left: %s", location.c_str());

  {
    ScopedLock lock(m_mutex);
    RangeServerStatePtr rs_state;
    RangeServerStateMap::iterator iter = m_server_map.find(location);

    {
      ScopedLock init_lock(m_root_server_mutex);
      if (location == m_root_server_location) {
	m_root_server_connected = false;
	memset(&m_root_server_addr, 0, sizeof(m_root_server_addr));
      }
    }

    if (iter == m_server_map.end()) {
      HT_WARNF("Server (%s) not found in map", location.c_str());
      return;
    }

    rs_state = (*iter).second;

    // if we're about to delete the item pointing to the server map iterator,
    // then advance the iterator
    if (iter == m_server_map_iter)
      ++m_server_map_iter;

    m_addr_map.erase(rs_state->connection);
    m_server_map.erase(iter);

    if (m_server_map.empty())
      m_no_servers_cond.notify_all();

    connection = rs_state->connection;
    was_connected = rs_state->connected;
    rs_state->connected = false;
  }

  if (was_connected)
    m_conn_manager_ptr->get_comm()->close_socket(connection);

  // delete Hyperspace file
  HT_INFOF("RangeServer lost it's lock on file %s, deleting ...", hsfname.c_str());
  try {
    m_hyperspace_ptr->unlink(hsfname);
  }
  catch (Exception &e) {
    HT_WARN_OUT "Problem closing file '" << hsfname << "' - " << e << HT_END;
  }

  /**
   *  Do (or schedule) tablet re-assignment here
   */
}



/**
 *
 */
void
Master::create_table(ResponseCallback *cb, const char *tablename,
                     const char *schemastr) {

  HT_INFOF("Create table: %s", tablename);
  wait_for_root_metadata_server();

  try {
    create_table(tablename, schemastr);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
    return;
  }

  cb->response_ok();
}

bool Master::table_exists(const String &name, String &id) {
  bool is_namespace;
  uint64_t handle = 0;
  HandleCallbackPtr null_handle_callback;

  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace_ptr, &handle);

  id = "";

  if (!m_namemap->name_to_id(name, id, &is_namespace) ||
      is_namespace)
    return false;

  String tablefile = m_toplevel_dir + "/tables/" + id;

  try {
    if (m_hyperspace_ptr->exists(tablefile)) {
      handle = m_hyperspace_ptr->open(tablefile, OPEN_FLAG_READ, null_handle_callback);
      if (m_hyperspace_ptr->attr_exists(handle, "x"))
        return true;
    }
  }
  catch (Exception &e) {
    if (e.code() == Error::HYPERSPACE_FILE_NOT_FOUND ||
        e.code() == Error::HYPERSPACE_BAD_PATHNAME)
      return false;
    HT_ERROR_OUT << e << HT_END;
    return false;
  }
  return false;
}

namespace {
  typedef hash_map<String, InetAddr> ConnectionMap;
}

/**
 *
 */
void
Master::alter_table(ResponseCallback *cb, const char *tablename,
                    const char *schemastr) {
  String finalschema = "";
  String err_msg = "";
  String table_id;
  SchemaPtr updated_schema;
  SchemaPtr schema;
  DynamicBuffer value_buf(0);
  uint64_t handle = 0;
  LockSequencer lock_sequencer;
  int saved_error = Error::OK;

  HandleCallbackPtr null_handle_callback;

  HT_INFOF("Alter table: %s", tablename);

  wait_for_root_metadata_server();

  try {
    String tablefile;

    if (!table_exists(tablename, table_id))
      HT_THROW(Error::TABLE_NOT_FOUND, tablename);

    tablefile = m_toplevel_dir + "/tables/" + table_id;

    /**
     *  Parse new schema & check validity
     */
    updated_schema = Schema::new_instance(schemastr, strlen(schemastr),
        true);
    if (!updated_schema->is_valid())
      HT_THROW(Error::MASTER_BAD_SCHEMA, updated_schema->get_error_string());

    /**
     *  Open & Lock Hyperspace file exclusively
     */
    handle = m_hyperspace_ptr->open(tablefile,
        OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_LOCK_EXCLUSIVE,
        null_handle_callback);

    /**
     *  Read existing schema and table id
     */

    m_hyperspace_ptr->attr_get(handle, "schema", value_buf);
    schema = Schema::new_instance((char *)value_buf.base,
        strlen((char *)value_buf.base), true);
    value_buf.clear();

    /**
     * Check if proposed schema generation is correct
     */
    uint32_t generation =  schema->get_generation()+1;
    if (updated_schema->get_generation() != generation) {
      HT_THROW(Error::MASTER_SCHEMA_GENERATION_MISMATCH,
          (String) "Expected updated schema generation " + generation
          + " got " + updated_schema->get_generation());
    }

    /**
     * Send updated schema to all RangeServers handling this table
     */
    finalschema = schemastr;
    {
      char start_row[16];
      char end_row[16];
      TableScannerPtr scanner_ptr;
      ScanSpec scan_spec;
      Cell cell;
      String location_str;
      ConnectionMap connections;
      ConnectionMap::iterator cmiter;
      RangeServerStateMap::iterator smiter;
      TableIdentifierManaged table;
      RowInterval ri;

      table.set_id(table_id);
      table.generation = 0;

      sprintf(start_row, "%s:", table_id.c_str());
      sprintf(end_row, "%s:%s", table_id.c_str(), Key::END_ROW_MARKER);

      scan_spec.row_limit = 0;
      scan_spec.max_versions = 1;
      scan_spec.columns.clear();
      scan_spec.columns.push_back("Location");

      ri.start = start_row;
      ri.end = end_row;
      scan_spec.row_intervals.push_back(ri);

      scanner_ptr = m_metadata_table_ptr->create_scanner(scan_spec);

      while (scanner_ptr->next(cell)) {
	location_str = String((const char *)cell.value, cell.value_len);
	boost::trim(location_str);
	if (connections.find(location_str) == connections.end()) {
	  ScopedLock lock(m_mutex);
	  if ((smiter = m_server_map.find(location_str)) == m_server_map.end()) {
	    /** Alter failed clean up & return **/
	    saved_error = Error::RANGESERVER_UNAVAILABLE;
	    err_msg = location_str;
	    HT_ERRORF("ALTER TABLE failed '%s' - %s", err_msg.c_str(),
		      Error::get_text(saved_error));
	    break;
	  }
	  connections[location_str] = (*smiter).second->connection;
	}
      }

      if (saved_error != Error::OK) {
	cb->error(saved_error, err_msg);
	m_hyperspace_ptr->close(handle);
	return;
      }

      if (!connections.empty()) {
        DispatchHandlerUpdateSchema sync_handler(table,
            finalschema.c_str(), m_conn_manager_ptr->get_comm(), 5000);
        RangeServerStatePtr state_ptr;

	// Issue ALTER TABLE commands to RangeServers
	for (cmiter = connections.begin(); cmiter != connections.end(); ++cmiter) {
	  CommAddress addr;
	  addr.set_proxy((*cmiter).first);
	  sync_handler.add((*cmiter).second);
	}

        if (!sync_handler.wait_for_completion()) {
          std::vector<DispatchHandlerUpdateSchema::ErrorResult> errors;
          uint32_t retry_count = 0;
          bool retry_failed;
          do {
            retry_count++;
            sync_handler.get_errors(errors);
            for (size_t i=0; i<errors.size(); i++) {
              HT_ERRORF("update schema error - %s - %s",
                  errors[i].msg.c_str(), Error::get_text(errors[i].error));
            }
            sync_handler.retry();
          }
          while ((retry_failed = (!sync_handler.wait_for_completion())) &&
              retry_count < MAX_ALTER_TABLE_RETRIES);
          /**
           * Alter table failed.. die for now
           */
          if (retry_failed) {
            sync_handler.get_errors(errors);
            String error_str;
            for (size_t i=0; i<errors.size(); i++) {
              error_str += (String) "update schema error '" +
                  errors[i].msg.c_str() + "' '" +
                  Error::get_text(errors[i].error) + "'";
              HT_FATALF("Maximum alter table attempts reached %d - %s",
                  MAX_ALTER_TABLE_RETRIES, error_str.c_str());
            }
          }
        }
      }

      /**
       * Store updated Schema in Hyperspace, close handle & release lock
       */
      HT_INFO_OUT <<"schema:\n"<< finalschema << HT_END;
      m_hyperspace_ptr->attr_set(handle, "schema", finalschema.c_str(),
                                 finalschema.length());
      /**
       * Alter succeeded so clean up!
       */
      m_hyperspace_ptr->close(handle);

      HT_INFOF("ALTER TABLE '%s' id=%s success",
               tablename, table_id.c_str());
    }
  }
  catch (Exception &e) {
    // clean up
    if(handle != 0)
      m_hyperspace_ptr->close(handle);

    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
    return;
  }

  cb->response_ok();
}

/**
 *
 */
void Master::get_schema(ResponseCallbackGetSchema *cb, const char *tablename) {
  String table_id;
  String errmsg;
  DynamicBuffer schemabuf(0);
  uint64_t handle;
  HandleCallbackPtr null_handle_callback;

  HT_INFOF("Get schema: %s", tablename);

  wait_for_root_metadata_server();

  try {
    String tablefile;


    if (!table_exists(tablename, table_id)) {
      cb->error(Error::TABLE_NOT_FOUND, tablename);
      return;
    }

    tablefile = m_toplevel_dir + "/tables/" + table_id;

    /**
     * Open table file
     */
    handle = m_hyperspace_ptr->open(tablefile, OPEN_FLAG_READ,
                                    null_handle_callback);

    /**
     * Get schema attribute
     */
    m_hyperspace_ptr->attr_get(handle, "schema", schemabuf);

    m_hyperspace_ptr->close(handle);

    cb->response((char *)schemabuf.base);

    if (m_verbose) {
      HT_INFOF("Successfully fetched schema (length=%d) for table '%s'",
               (int)strlen((char *)schemabuf.base), tablename);
    }

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
  }
}



/**
 *
 */
void
Master::register_server(ResponseCallbackRegisterServer *cb, String &location,
                        uint16_t listen_port, StatsSystem &system_stats) {
  RangeServerStateMap::iterator iter;
  HandleCallbackPtr lock_file_handler;
  LockSequencer lock_sequencer;
  String hsfname;
  bool exists = false;
  InetAddr connection = cb->get_address();
  InetAddr addr = InetAddr(system_stats.net_info.primary_addr, listen_port);
  String addr_str = InetAddr::format(addr);

  if (location == "") {
    {
      ScopedLock lock(m_mutex);
      location = String("rs") + m_next_server_id++;
    }
    char buf[16];
    sprintf(buf, "%u", m_next_server_id);
    m_hyperspace_ptr->attr_set(m_master_file_handle, "next_server_id",
                               buf, strlen(buf)+1);
    m_hyperspace_ptr->attr_set(m_servers_dir_handle, location,
                               addr_str.c_str(), addr_str.length()+1);
  }

  HT_INFOF("Register server %s (%s -> %s)", connection.format().c_str(),
           location.c_str(), addr_str.c_str());

  m_monitoring->add_server(location, system_stats);

  try {

    {
      ScopedLock lock(m_mutex);
      RangeServerStatePtr rs_state;

      if((iter = m_server_map.find(location)) != m_server_map.end()) {
        rs_state = (*iter).second;
        HT_ERRORF("Unable to assign %s to location '%s' because already assigned to %s",
                  addr_str.c_str(), location.c_str(),
                  InetAddr::format(rs_state->connection).c_str());
        cb->error(Error::MASTER_LOCATION_ALREADY_ASSIGNED,
                  format("location '%s' already assigned to %s", location.c_str(),
                         InetAddr::format(rs_state->connection).c_str()));
        return;
      }
      else {
        rs_state = new RangeServerState();
        rs_state->location = location;
        rs_state->addr.set_proxy(location);
      }

      m_conn_manager_ptr->get_comm()->set_alias(connection, addr);
      m_conn_manager_ptr->get_comm()->add_proxy(location, addr);

      rs_state->connection = connection;
      rs_state->connected = true;

      hsfname = m_toplevel_dir + "/servers/" + location;

      m_server_map[rs_state->location] = rs_state;
      m_addr_map[rs_state->connection] = location;
    }

    HT_INFOF("Server Registered %s -> %s", location.c_str(), addr_str.c_str());

    cb->response(location);

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
    return;
  }

  /**
   * TEMPORARY: Load root and second-level METADATA ranges
   */
  {
    ScopedLock init_lock(m_root_server_mutex);

    if (!m_initialized) {
      TableIdentifier table;
      RangeSpec range;
      RangeServerClient rsc(m_conn_manager_ptr->get_comm());

      HT_INFO("Initializing METADATA");

      /**
       *  Create "sys" namespace
       */
      try {
        create_namespace("sys");
      }
      catch (Exception &e) {
        if (e.code() != Error::NAMESPACE_EXISTS) {
          HT_ERROR_OUT << e << HT_END;
          HT_ABORT;
        }
      }

      /**
       * Create sys/METADATA table
       */
      {
        String metadata_schema_file = System::install_dir
                                      + "/conf/METADATA.xml";
        off_t schemalen;
        const char *schemastr =
          FileUtils::file_to_buffer(metadata_schema_file.c_str(), &schemalen);

        try {
          create_table(TableIdentifier::METADATA_NAME, schemastr);
        }
        catch (Exception &e) {
          if (e.code() != Error::MASTER_TABLE_EXISTS) {
            HT_ERROR_OUT << e << HT_END;
            HT_ABORT;
          }
          exists = true;
        }
      }

      /**
       * Open METADATA table
       */
      m_metadata_table_ptr = new Table(m_props_ptr, m_conn_manager_ptr,
                                       m_hyperspace_ptr, m_namemap,
                                       TableIdentifier::METADATA_NAME);

      /**
       * If table exists, then ranges should already have been assigned,
       * so figure out the location of the root METADATA server, and
       * set the root_server_connected flag appropriately
       */
      if (exists) {
        DynamicBuffer dbuf;
        try {
          HandleCallbackPtr null_callback;
          uint64_t handle = m_hyperspace_ptr->open(m_toplevel_dir + "/root",
              OPEN_FLAG_READ, null_callback);
          m_hyperspace_ptr->attr_get(handle, "Location", dbuf);
          m_hyperspace_ptr->close(handle);
        }
        catch (Exception &e) {
          HT_FATALF("Unable to read '%s/root:Location' in hyperspace "
                    "- %s - %s,", m_toplevel_dir.c_str(),
                    Error::get_text(e.code()), e.what());
        }
        m_root_server_location = (const char *)dbuf.base;
        if (m_root_server_location == location) {
          m_root_server_connected = true;
          m_root_server_addr = cb->get_address();
        }
        m_initialized = true;
        m_root_server_cond.notify_all();
        HT_INFO("METADATA table already exists");
        return;
      }

      m_metadata_table_ptr->get_identifier(&table);

      /**
       * Load root METADATA range
       */
      range.start_row = 0;
      range.end_row = Key::END_ROOT_ROW;

      try {
        RangeState range_state;
        range_state.soft_limit = m_max_range_bytes;
        rsc.load_range(connection, table, range, 0, range_state);
      }
      catch (Exception &e) {
        HT_ERRORF("Problem issuing 'load range' command for %s[..%s] at server "
                  "%s - %s", table.id, range.end_row,
                  location.c_str(), Error::get_text(e.code()));
      }

      /**
       * Write METADATA entry for second-level METADATA range
       */

      TableMutatorPtr mutator_ptr;
      KeySpec key;
      String metadata_key_str;

      mutator_ptr = m_metadata_table_ptr->create_mutator();

      metadata_key_str = (String)TableIdentifier::METADATA_ID +":"+ Key::END_ROW_MARKER;
      key.row = metadata_key_str.c_str();
      key.row_len = metadata_key_str.length();
      key.column_qualifier = 0;
      key.column_qualifier_len = 0;

      try {
        key.column_family = "StartRow";
        mutator_ptr->set(key, (uint8_t *)Key::END_ROOT_ROW,
                         strlen(Key::END_ROOT_ROW));
        mutator_ptr->flush();
      }
      catch (Hypertable::Exception &e) {
        // TODO: propagate exception
        HT_ERRORF("METADATA update error (row_key = %s) - %s : %s",
                  metadata_key_str.c_str(), e.what(),
                  Error::get_text(e.code()));
        exit(1);
      }

      /**
       * Load second-level METADATA range
       */
      range.start_row = Key::END_ROOT_ROW;
      range.end_row = Key::END_ROW_MARKER;

      try {
        RangeState range_state;
        range_state.soft_limit = m_max_range_bytes;
        rsc.load_range(connection, table, range, 0, range_state);
      }
      catch (Exception &e) {
        HT_ERRORF("Problem issuing 'load range' command for %s[..%s] at server "
                  "%s - %s", table.id, range.end_row,
                  location.c_str(), Error::get_text(e.code()));
      }

      HT_INFO("sys/METADATA table successfully initialized");

      /**
       * Create sys/RS_METRICS table
       */
      {
        String rs_stats_schema_file = System::install_dir+"/conf/RS_METRICS.xml";
        off_t schemalen;
        const char *schemastr = FileUtils::file_to_buffer(rs_stats_schema_file.c_str(), &schemalen);

        try {
          create_table("sys/RS_METRICS", schemastr);
          HT_INFO("sys/RS_METRICS table successfully created");
        }
        catch (Exception &e) {
          if (e.code() != Error::MASTER_TABLE_EXISTS) {
            HT_ERROR_OUT << e << HT_END;
            HT_ABORT;
          }
        }
      }

      m_root_server_location = location;
      m_root_server_addr = addr;
      m_root_server_connected = true;
      m_initialized = true;
      m_root_server_cond.notify_all();
    }
    else if (!m_root_server_connected && location == m_root_server_location) {
      m_root_server_connected = true;
      m_root_server_addr = addr;
      m_root_server_cond.notify_all();
    }
  }

}

/**
 * TEMPORARY: Just turns around and assigns new range to caller
 *
 * NOTE: this call can't be protected by a mutex because it can cause the
 * whole system to wedge under certain situations
 */
void
Master::move_range(ResponseCallback *cb, const TableIdentifier &table,
                   const RangeSpec &range, const char *transfer_log_dir,
                   uint64_t soft_limit, bool split) {
  RangeServerClient rsc(m_conn_manager_ptr->get_comm());
  QualifiedRangeSpec fqr_spec(table, range);
  bool server_pinned = false;
  String location;
  CommAddress addr;

  HT_INFOF("Entering move_range for %s[%s:%s] (split=%d).", table.id, range.start_row,
           range.end_row, (int)split);

  wait_for_root_metadata_server();

  {
    ScopedLock lock(m_mutex);
    RangeToLocationMap::iterator iter = m_range_to_location_map.find(fqr_spec);
    if (iter != m_range_to_location_map.end()) {
      location = (*iter).second;
      RangeServerStateMap::iterator smiter = m_server_map.find(location);
      if (smiter == m_server_map.end() || !(*smiter).second->connected) {
        cb->error(Error::COMM_NOT_CONNECTED, location);
        return;
      }
      addr = (*smiter).second->addr;
      HT_INFOF("Re-attempting to assign newly reported range %s[%s:%s] to %s",
               table.id, range.start_row, range.end_row, location.c_str());
      server_pinned = true;
    }
    else {
      if (m_server_map_iter == m_server_map.end())
        m_server_map_iter = m_server_map.begin();
      assert(m_server_map_iter != m_server_map.end());
      location = (*m_server_map_iter).second->location;
      addr = (*m_server_map_iter).second->addr;
      HT_INFOF("Assigning newly reported range %s[%s:%s] to %s",
               table.id, range.start_row, range.end_row, location.c_str());
      ++m_server_map_iter;
    }
  }


  try {
    RangeState range_state;
    range_state.soft_limit = soft_limit;
    rsc.load_range(addr, table, range, transfer_log_dir, range_state);
    HT_INFOF("move_range for %s[%s:%s] successful.", table.id,
             range.start_row, range.end_row);
  }
  catch (Exception &e) {
    if (e.code() == Error::RANGESERVER_RANGE_ALREADY_LOADED) {
      HT_ERROR_OUT << e << HT_END;
      {
	ScopedLock lock(m_mutex);
	m_range_to_location_map.erase(fqr_spec);
      }
      cb->response_ok();
    }
    else {
      HT_ERRORF("Problem issuing 'load range' command for %s[%s:%s] at server "
                "%s - %s", table.id, range.start_row, range.end_row,
                location.c_str(), Error::get_text(e.code()));
      {
	ScopedLock lock(m_mutex);
	if (!server_pinned)
	  m_range_to_location_map[fqr_spec] = location;
      }
      cb->error(e.code(), e.what());
    }
    return;
  }

  if (server_pinned) {
    ScopedLock lock(m_mutex);
    m_range_to_location_map.erase(fqr_spec);
  }

  cb->response_ok();
}

void
Master::rename_table(ResponseCallback *cb, const char *old_name, const char *new_name) {

  HT_INFOF("Entering rename table from %s to %s", old_name, new_name);
  String table_id;

  try {
    if (!table_exists(old_name, table_id)) {
      HT_THROW(Error::TABLE_NOT_FOUND, old_name);
    }
    // TODO: log the 'STARTED RENAME TABLE fully_qual_name' in the Master METALOG here
    m_namemap->rename(old_name, new_name);
    HT_INFOF("RENAME TABLE '%s' -> '%s' success", old_name, new_name);
    cb->response_ok();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
  }
}

void
Master::drop_table(ResponseCallback *cb, const char *table_name,
                   bool if_exists) {
  int saved_error = Error::OK;
  String err_msg;
  String table_id;
  DynamicBuffer value_buf(0);
  HandleCallbackPtr null_handle_callback;
  String table_name_str = table_name;

  HT_INFOF("Entering drop_table for %s", table_name);

  wait_for_root_metadata_server();

  try {

    if (!table_exists(table_name, table_id)) {
      if (if_exists) {
        cb->response_ok();
        return;
      }
      HT_THROW(Error::TABLE_NOT_FOUND, table_name);
    }

    {
      char start_row[16];
      char end_row[16];
      TableScannerPtr scanner_ptr;
      ScanSpec scan_spec;
      Cell cell;
      String location_str;
      ConnectionMap connections;
      ConnectionMap::iterator cmiter;
      RangeServerStateMap::iterator smiter;
      TableIdentifierManaged table;
      RowInterval ri;

      table.set_id(table_id);
      table.generation = 0;

      sprintf(start_row, "%s:", table_id.c_str());
      sprintf(end_row, "%s:%s", table_id.c_str(), Key::END_ROW_MARKER);

      scan_spec.row_limit = 0;
      scan_spec.max_versions = 1;
      scan_spec.columns.clear();
      scan_spec.columns.push_back("Location");

      ri.start = start_row;
      ri.end = end_row;
      scan_spec.row_intervals.push_back(ri);

      scanner_ptr = m_metadata_table_ptr->create_scanner(scan_spec);

      while (scanner_ptr->next(cell)) {
	location_str = String((const char *)cell.value, cell.value_len);
	boost::trim(location_str);
	if (connections.find(location_str) == connections.end()) {
	  ScopedLock lock(m_mutex);
	  if ((smiter = m_server_map.find(location_str)) == m_server_map.end()) {
	    /** Drop failed clean up & return **/
	    saved_error = Error::RANGESERVER_UNAVAILABLE;
	    err_msg = location_str;
	    HT_ERRORF("DROP TABLE failed '%s' - %s", err_msg.c_str(),
		      Error::get_text(saved_error));
	    break;
	  }
	  connections[location_str] = (*smiter).second->connection;
	}
      }

      if (saved_error != Error::OK) {
	cb->error(saved_error, err_msg);
	return;
      }

      if (!connections.empty()) {
	DispatchHandlerDropTable sync_handler(table, m_conn_manager_ptr->get_comm());
        RangeServerStatePtr state_ptr;

	// Issue DROP TABLE commands to RangeServers
	for (cmiter = connections.begin(); cmiter != connections.end(); ++cmiter) {
	  CommAddress addr;
	  addr.set_proxy((*cmiter).first);
	  sync_handler.add((*cmiter).second);
	}

        if (!sync_handler.wait_for_completion()) {
          std::vector<DispatchHandlerDropTable::ErrorResult> errors;
          sync_handler.get_errors(errors);
          for (size_t i=0; i<errors.size(); i++) {
            HT_WARNF("drop table error - %s - %s", errors[i].msg.c_str(),
                     Error::get_text(errors[i].error));
          }
          cb->error(errors[0].error, errors[0].msg);
          return;
        }
      }

    }

    m_namemap->drop_mapping(table_name);

    String table_file = m_toplevel_dir + "/tables/" + table_id;
    m_hyperspace_ptr->unlink(table_file.c_str());

    HT_INFOF("DROP TABLE '%s' id=%s success",
             table_name_str.c_str(), table_id.c_str());

    cb->response_ok();

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
  }
}

  void Master::close(ResponseCallback *cb) {
    RangeServerClient rsc(m_conn_manager_ptr->get_comm());
    std::vector<CommAddress> addresses;

    HT_INFO("CLOSE");

    {
      ScopedLock lock(m_mutex);
      addresses.reserve(m_server_map.size());
      for (RangeServerStateMap::iterator iter = m_server_map.begin();
	   iter != m_server_map.end(); ++iter)
	addresses.push_back((*iter).second->addr);
    }

    for (size_t i=0; i<addresses.size(); i++)
      rsc.close(addresses[i]);

    cb->response_ok();
  }


  void Master::shutdown(ResponseCallback *cb) {
    RangeServerClient rsc(m_conn_manager_ptr->get_comm());
    std::vector<CommAddress> addresses;

    HT_INFO("SHUTDOWN");

    {
      ScopedLock lock(m_mutex);
      addresses.reserve(m_server_map.size());
      for (RangeServerStateMap::iterator iter = m_server_map.begin();
	   iter != m_server_map.end(); ++iter)
	addresses.push_back((*iter).second->addr);
    }

    // issue shutdown commands
    for (size_t i=0; i<addresses.size(); i++)
      rsc.shutdown(addresses[i]);

    {
      ScopedLock lock(m_mutex);
      boost::xtime expire_time;
      boost::xtime_get(&expire_time, boost::TIME_UTC);
      expire_time.sec += (int64_t)30;
      m_no_servers_cond.timed_wait(lock, expire_time);
    }

    int server_map_size;

    {
      ScopedLock lock(m_mutex);
      server_map_size = m_server_map.size();
    }

    if (server_map_size != 0) {
      String err_msg = format("%d RangeServers failed to shutdown",
			      server_map_size);
      cb->error(Error::REQUEST_TIMEOUT, err_msg);
      return;
    }

    m_hyperspace_ptr = 0;

    cb->response_ok();

    poll(0, 0, 1000);

    _exit(0);

  }

  void Master::do_maintenance() {
    do_monitoring();
  }


void
Master::create_table(const char *tablename, const char *schemastr) {
  String finalschema = "";
  string table_basedir;
  string agdir;
  Schema *schema = 0;
  HandleCallbackPtr null_handle_callback;
  uint64_t handle = 0;
  String table_name = tablename;
  String table_id;

  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace_ptr, &handle);

  /**
   * Strip leading '/'
   */
  if (table_name[0] == '/')
    table_name = table_name.substr(1);

  if (table_exists(table_name, table_id))
    HT_THROW(Error::MASTER_TABLE_EXISTS, table_name);

  /**
   * Add Name-to-ID mapping
   */
  if (table_id == "")
    m_namemap->add_mapping(table_name, table_id);

  HT_ASSERT(table_name != TableIdentifier::METADATA_NAME ||
            table_id == TableIdentifier::METADATA_ID);

  /**
   *  Parse Schema and assign Generation number and Column ids
   */
  schema = Schema::new_instance(schemastr, strlen(schemastr));
  if (!schema->is_valid())
    HT_THROW(Error::MASTER_BAD_SCHEMA, schema->get_error_string());

  schema->assign_ids();
  schema->render(finalschema);
  HT_DEBUG_OUT <<"schema:\n"<< finalschema << HT_END;

  /**
   * Create table file
   */
  String tablefile = m_toplevel_dir + "/tables/" + table_id;
  int oflags = OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE;
  handle = m_hyperspace_ptr->open(tablefile, oflags, null_handle_callback);

  /**
   * Write schema attribute
   */
  m_hyperspace_ptr->attr_set(handle, "schema", finalschema.c_str(),
                             finalschema.length());

  // TODO: log the 'COMPLETED CREATE TABLE fully_qual_name' in the Master METALOG here

  /**
   * Create /hypertable/tables/&lt;table&gt;/&lt;accessGroup&gt; directories
   * for this table in DFS
   */
  table_basedir = m_toplevel_dir + "/tables/" + table_id + "/";

  foreach(const Schema::AccessGroup *ag, schema->get_access_groups()) {
    agdir = table_basedir + ag->name;
    m_dfs_client->mkdirs(agdir);
  }

  /**
   * Write METADATA entry, single range covering entire table '\\0' to 0xff 0xff
   */
  if (table_id != TableIdentifier::METADATA_ID) {
    TableMutatorPtr mutator_ptr;
    KeySpec key;
    String metadata_key_str;
    CommAddress addr;
    String location;

    mutator_ptr = m_metadata_table_ptr->create_mutator();

    metadata_key_str = table_id + ":" + Key::END_ROW_MARKER;
    key.row = metadata_key_str.c_str();
    key.row_len = metadata_key_str.length();
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;

    key.column_family = "StartRow";
    mutator_ptr->set(key, 0, 0);
    mutator_ptr->flush();

    /**
     * TEMPORARY:  ask the one Range Server that we know about to load the range
     */

    TableIdentifierManaged table;
    RangeSpec range;
    uint64_t soft_limit;
    RangeServerClient rsc(m_conn_manager_ptr->get_comm());

    table.set_id(table_id);
    table.generation = schema->get_generation();

    range.start_row = 0;
    range.end_row = Key::END_ROW_MARKER;

    {
      ScopedLock lock(m_mutex);
      if (m_server_map_iter == m_server_map.end())
        m_server_map_iter = m_server_map.begin();
      assert(m_server_map_iter != m_server_map.end());
      addr = (*m_server_map_iter).second->addr;
      location = (*m_server_map_iter).second->location;
      HT_INFOF("Assigning first range %s[:%s] to %s", table.id,
	       range.end_row, location.c_str());
      ++m_server_map_iter;
      soft_limit = m_max_range_bytes / std::min(64, (int)m_server_map.size()*2);
    }

    try {
      RangeState range_state;
      range_state.soft_limit = soft_limit;
      rsc.load_range(addr, table, range, 0, range_state);
    }
    catch (Exception &e) {
      String err_msg = format("Problem issuing 'load range' command for "
          "%s[..%s] at server %s - %s", table.id, range.end_row,
          location.c_str(), Error::get_text(e.code()));
      if (schema != 0)
        delete schema;
      HT_THROW2(e.code(), e, err_msg);
    }
  }

  m_hyperspace_ptr->attr_set(handle, "x", "", 0);


  delete schema;
  if (m_verbose)
    HT_INFOF("Successfully created table '%s' ID=%s", tablename, table_id.c_str());

}

/**
 * PRIVATE Methods
 */

bool Master::initialize() {
  uint64_t handle;
  HandleCallbackPtr null_handle_callback;

  try {

    if (!m_hyperspace_ptr->exists(m_toplevel_dir))
      m_hyperspace_ptr->mkdirs(m_toplevel_dir);

    if (!m_hyperspace_ptr->exists(m_toplevel_dir + "/servers")) {
      if (!create_hyperspace_dir(m_toplevel_dir + "/servers"))
        return false;
    }

    if (!m_hyperspace_ptr->exists(m_toplevel_dir + "/tables")) {
      if (!create_hyperspace_dir(m_toplevel_dir + "/tables"))
        return false;
    }

    // Create /hypertable/master if necessary
    handle = m_hyperspace_ptr->open( m_toplevel_dir + "/master",
        OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE, null_handle_callback);
    m_hyperspace_ptr->close(handle);

    /**
     *  Create /hypertable/root
     */
    handle = m_hyperspace_ptr->open(m_toplevel_dir + "/root",
        OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE, null_handle_callback);
    m_hyperspace_ptr->close(handle);

    HT_INFO("Successfully Initialized Hypertable.");

    return true;

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return false;
  }
}


/**
 *
 */
void Master::scan_servers_directory() {
  ScopedLock lock(m_mutex);
  HandleCallbackPtr lock_file_handler;
  std::vector<struct DirEntry> listing;
  uint32_t lock_status;
  LockSequencer lock_sequencer;
  RangeServerStatePtr rs_state;
  uint32_t oflags;
  String hsfname;
  uint64_t hyperspace_handle;
  std::vector<String> names;

  try {

    /**
     * Open /hyperspace/servers directory and scan for range servers
     */
    m_servers_dir_callback_ptr =
        new ServersDirectoryHandler(this, m_app_queue_ptr);

    m_servers_dir_handle = m_hyperspace_ptr->open(m_toplevel_dir + "/servers",
        OPEN_FLAG_READ, m_servers_dir_callback_ptr);

    m_hyperspace_ptr->attr_list(m_servers_dir_handle, names);
    for (size_t i=0; i<names.size(); i++)
      HT_INFOF("Mapping: %s", names[i].c_str());

    m_hyperspace_ptr->readdir(m_servers_dir_handle, listing);

    oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;

    for (size_t i=0; i<listing.size(); i++) {

      rs_state = new RangeServerState();
      rs_state->location = listing[i].name;
      rs_state->addr.set_proxy(listing[i].name);

      hsfname = m_toplevel_dir + "/servers/" + listing[i].name;

      lock_file_handler =
          new ServerLockFileHandler(rs_state, this, m_app_queue_ptr);

      hyperspace_handle =
          m_hyperspace_ptr->open(hsfname, oflags, lock_file_handler);

      m_hyperspace_ptr->try_lock(hyperspace_handle,
          LOCK_MODE_EXCLUSIVE, &lock_status, &lock_sequencer);

      if (lock_status == LOCK_STATUS_GRANTED) {
        HT_INFOF("Obtained lock on servers file %s, removing...",
                 hsfname.c_str());
        m_hyperspace_ptr->close(hyperspace_handle);
        m_hyperspace_ptr->unlink(hsfname);
      }
      else {
        m_hyperspace_ptr->close(hyperspace_handle);
        m_server_map[rs_state->location] = rs_state;
      }
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    HT_ABORT;
  }
}


/**
 *
 */
bool Master::create_hyperspace_dir(const String &dir) {

  try {

    if (m_hyperspace_ptr->exists(dir))
      return true;

    m_hyperspace_ptr->mkdir(dir);

  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Problem creating hyperspace directory '" << dir << "'"
        << HT_END;
    HT_ERROR_OUT << e << HT_END;
    return false;
  }

  return true;
}

bool Master::handle_disconnect(struct sockaddr_in addr, String &location) {
  ScopedLock lock(m_mutex);
  SockAddrMap<String>::iterator iter = m_addr_map.find(addr);
  if (iter == m_addr_map.end())
    return false;
  location = (*iter).second;
  RangeServerStatePtr rs_state = m_server_map[location];
  if (rs_state)
    rs_state->connected = false;
  return true;
}

void Master::join() {
  m_app_queue_ptr->join();
  m_threads.join_all();
}

void Master::wait_for_root_metadata_server() {
  ScopedLock lock(m_root_server_mutex);
  while (!m_root_server_connected) {
    HT_WARN("Waiting for root metadata server ...");
    m_root_server_cond.wait(lock);
  }
}

/**
 *
 */
void
Master::create_namespace(ResponseCallback *cb, const char *name, int flags) {

  HT_INFOF("Create namespace: %s", name);
  try {
    create_namespace(name, flags);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
    return;
  }

  cb->response_ok();

}

/**
 *
 */
void
Master::create_namespace(const char *name, int flags) {

  String ns(name);
  String id;
  flags |= NameIdMapper::IS_NAMESPACE;
  String hyperspace_tables_dir;

  // TODO: log 'CREATE NAMESPACE fully_qual_name' in the Master METALOG here
  m_namemap->add_mapping(ns, id, flags);
  hyperspace_tables_dir = m_toplevel_dir + "/tables/" + id;

  if (flags & NameIdMapper::CREATE_INTERMEDIATE)
    m_hyperspace_ptr->mkdirs(hyperspace_tables_dir);
  else
    m_hyperspace_ptr->mkdir(hyperspace_tables_dir);

  HT_INFO_OUT << "Created namespace mapping " << ns << "<->" << id << HT_END;

}
/**
 *
 */
void
Master::drop_namespace(ResponseCallback *cb, const char *name, bool if_exists) {

  HT_INFOF("Drop namespace: %s", name);
  String ns(name);
  String id, hyperspace_tables_dir;
  bool exists;
  bool is_namespace;
  // TODO: log 'DROP NAMESPACE fully_qual_name' in the Master METALOG here
  try {
    exists = m_namemap->name_to_id(ns, id, &is_namespace);

    if (!if_exists &&  (!exists || !is_namespace)) {
      HT_THROW(Error::NAMESPACE_DOES_NOT_EXIST, name);
    }
    else if (!exists && if_exists) {
      cb->response_ok();
      return;
    }

    m_namemap->drop_mapping(ns);
    hyperspace_tables_dir = m_toplevel_dir + "/tables/" + id;
    m_hyperspace_ptr->unlink(hyperspace_tables_dir);
  }
  catch (Exception &e){
    HT_ERROR_OUT << e << HT_END;
    cb->error(e.code(), e.what());
    return;
  }

  cb->response_ok();
}


/**
 *
 */
  void Master::do_monitoring() {
  std::vector<RangeServerStatistics> results;

  {
    ScopedLock lock(m_stats_mutex);
    if (m_get_stats_outstanding) {
      HT_WARN_OUT << "get_statistics request outstanding" << HT_END;
      return;
    }
    m_get_stats_outstanding = true;
  }

  wait_for_root_metadata_server();

  {
    ScopedLock lock(m_mutex);
    results.resize(m_server_map.size());
    size_t i=0;
    for (RangeServerStateMap::iterator iter = m_server_map.begin();
         iter != m_server_map.end(); ++iter) {
      results[i].location = (*iter).second->location;
      results[i].addr = (*iter).second->connection;
      i++;
    }
  }

  time_t timeout = m_maintenance_interval;
  if (timeout > 1000)
    timeout -= 1000;
  DispatchHandlerGetStatistics sync_handler(m_conn_manager_ptr->get_comm(), timeout);

  sync_handler.add(results);

  sync_handler.wait_for_completion();

  m_monitoring->add(results);

  {
    ScopedLock lock(m_stats_mutex);
    m_get_stats_outstanding = false;
  }

}


} // namespace Hypertable
