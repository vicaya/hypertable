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

#include "Common/Compat.h"

#include <algorithm>

extern "C" {
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
}

#include <boost/algorithm/string.hpp>

#include "Common/FileUtils.h"
#include "Common/InetAddr.h"
#include "Common/System.h"

#include "DfsBroker/Lib/Client.h"
#include "Hypertable/Lib/LocationCache.h"
#include "Hypertable/Lib/RangeServerClient.h"
#include "Hypertable/Lib/RangeState.h"
#include "Hypertable/Lib/Schema.h"
#include "Hyperspace/DirEntry.h"

#include "DropTableDispatchHandler.h"
#include "Master.h"
#include "ServersDirectoryHandler.h"
#include "ServerLockFileHandler.h"
#include "RangeServerState.h"

using namespace Hyperspace;
using namespace Hypertable;
using namespace Hypertable::DfsBroker;
using namespace std;

namespace Hypertable {

Master::Master(ConnectionManagerPtr &conn_mgr, PropertiesPtr &props_ptr, ApplicationQueuePtr &app_queue) : m_props_ptr(props_ptr), m_conn_manager_ptr(conn_mgr), m_app_queue_ptr(app_queue), m_verbose(false), m_dfs_client(0), m_initialized(false) {
  int error;
  Client *dfs_client;
  uint16_t port;

  m_server_map_iter = m_server_map.begin();

  m_hyperspace_ptr = new Hyperspace::Session(conn_mgr->get_comm(), props_ptr, &m_hyperspace_session_handler);

  if (!m_hyperspace_ptr->wait_for_connection(30)) {
    HT_ERROR("Unable to connect to hyperspace, exiting...");
    exit(1);
  }

  m_verbose = props_ptr->get_bool("Hypertable.Verbose", false);

  if ((port = (uint16_t)props_ptr->get_int("Hypertable.Master.Port", 0)) == 0) {
    HT_ERROR("Hypertable.Master.Port property not found in config file, exiting...");
    exit(1);
  }

  m_max_range_bytes = props_ptr->get_int64("Hypertable.RangeServer.Range.MaxBytes", 200000000LL);

  /**
   * Create DFS Client connection
   */
  dfs_client = new DfsBroker::Client(conn_mgr, props_ptr);

  if (m_verbose) {
    cout << "DfsBroker.Host=" << props_ptr->get("DfsBroker.Host", "") << endl;
    cout << "DfsBroker.Port=" << props_ptr->get("DfsBroker.Port", "") << endl;
    cout << "DfsBroker.Timeout=" << props_ptr->get("DfsBroker.Timeout", "") << endl;
  }

  if (!dfs_client->wait_for_connection(30)) {
    HT_ERROR("Unable to connect to DFS Broker, exiting...");
    exit(1);
  }

  m_dfs_client = dfs_client;

  atomic_set(&m_last_table_id, 0);

  if (!initialize())
    exit(1);

  /* Read Last Table ID */
  {
    DynamicBuffer valbuf(0);
    HandleCallbackPtr null_handle_callback;
    int ival;
    uint32_t lock_status;
    uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;

    if ((error = m_hyperspace_ptr->open("/hypertable/master", oflags, null_handle_callback, &m_master_file_handle)) != Error::OK) {
      HT_ERRORF("Unable to open Hyperspace file '/hypertable/master' (%s)  Try re-running with --initialize",
                   Error::get_text(error));
      exit(1);
    }

    if ((error = m_hyperspace_ptr->try_lock(m_master_file_handle, LOCK_MODE_EXCLUSIVE, &lock_status, &m_master_file_sequencer)) != Error::OK) {
      HT_ERRORF("Problem obtaining exclusive lock on master Hyperspace file '/hypertable/master' - %s", Error::get_text(error));
      exit(1);
    }

    if (lock_status != LOCK_STATUS_GRANTED) {
      HT_ERROR("Unable to obtain lock on '/hypertable/master' - conflict");
      exit(1);
    }

    // Write master location in 'address' attribute, format is IP:port
    {
      String host_str, addr_str;
      struct hostent *he;
      InetAddr::get_hostname(host_str);
      if ((he = gethostbyname(host_str.c_str())) == 0) {
        HT_ERRORF("Problem obtaining address for hostname '%s'", host_str.c_str());
        exit(1);
      }
      addr_str = String(inet_ntoa(*(struct in_addr *)*he->h_addr_list)) + ":" + (int)port;
      if ((error = m_hyperspace_ptr->attr_set(m_master_file_handle, "address", addr_str.c_str(), strlen(addr_str.c_str()))) != Error::OK) {
        HT_ERRORF("Problem setting attribute 'address' of hyperspace file /hypertable/master - %s", Error::get_text(error));
        exit(1);
      }
    }

    if ((error = m_hyperspace_ptr->attr_get(m_master_file_handle, "last_table_id", valbuf)) != Error::OK) {
      if (error == Error::HYPERSPACE_ATTR_NOT_FOUND) {
        uint32_t table_id = 0;
        if ((error = m_hyperspace_ptr->attr_set(m_master_file_handle, "last_table_id", &table_id, sizeof(int32_t))) != Error::OK) {
          HT_ERRORF("Problem setting attribute 'last_table_id' of file /hypertable/master - %s", Error::get_text(error));
          exit(1);
        }
        ival = 0;
      }
      else {
        HT_ERRORF("Problem getting attribute 'last_table_id' from file /hypertable/master - %s", Error::get_text(error));
        exit(1);
      }
    }
    else {
      assert(valbuf.fill() == sizeof(int32_t));
      memcpy(&ival, valbuf.base, sizeof(int32_t));
    }

    atomic_set(&m_last_table_id, ival);
    if (m_verbose)
      cout << "Last Table ID: " << ival << endl;
  }

  /**
   * Locate tablet servers
   */
  scan_servers_directory();

  master_gc_start(props_ptr, m_threads, m_metadata_table_ptr, m_dfs_client);
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
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  uint32_t lock_status;
  LockSequencer lock_sequencer;
  ServerMap::iterator iter = m_server_map.find(location);
  String hsfname = (String)"/hypertable/servers/" + location;

  if (iter == m_server_map.end()) {
    HT_WARNF("Server (%s) not found in map", location.c_str());
    return;
  }

  // if we're about to delete the item pointing to the server map iterator, then advance the iterator
  if (iter == m_server_map_iter)
    m_server_map_iter++;

  if ((error = m_hyperspace_ptr->try_lock((*iter).second->hyperspace_handle, LOCK_MODE_EXCLUSIVE, &lock_status, &lock_sequencer)) != Error::OK) {
    HT_ERRORF("Problem obtaining exclusive lock on master Hyperspace file '/hypertable/master' - %s", Error::get_text(error));
    return;
  }

  if (lock_status != LOCK_STATUS_GRANTED) {
    HT_INFOF("Unable to obtain lock on server file %s, ignoring...", location.c_str());
    return;
  }

  m_hyperspace_ptr->unlink(hsfname);
  m_hyperspace_ptr->close((*iter).second->hyperspace_handle);
  m_server_map.erase(iter);

  HT_INFOF("RangeServer lost it's lock on file %s, deleting ...", hsfname.c_str());
  cout << flush;

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
  int error;
  String errmsg;

  if ((error = create_table(tablename, schemastr, errmsg)) != Error::OK)
    cb->error(error, errmsg);
  else
    cb->response_ok();
}


/**
 *
 */
void Master::get_schema(ResponseCallbackGetSchema *cb, const char *tablename) {
  int error = Error::OK;
  String tablefile = (String)"/hypertable/tables/" + tablename;
  String errmsg;
  DynamicBuffer schemabuf(0);
  uint64_t handle;
  bool exists;
  HandleCallbackPtr null_handle_callback;

  /**
   * Check for table existence
   */
  if ((error = m_hyperspace_ptr->exists(tablefile, &exists)) != Error::OK) {
    errmsg = (String)"Problem checking for existence of table '" + tablename + "' - " + Error::get_text(error);
    goto abort;
  }

  /**
   * Open table file
   */
  if ((error = m_hyperspace_ptr->open(tablefile, OPEN_FLAG_READ, null_handle_callback, &handle)) != Error::OK) {
    errmsg = "Unable to open Hyperspace table file '" + tablefile + "' (" + Error::get_text(error) + ")";
    goto abort;
  }

  /**
   * Get schema attribute
   */
  if ((error = m_hyperspace_ptr->attr_get(handle, "schema", schemabuf)) != Error::OK) {
    errmsg = "Problem getting attribute 'schema' for table file '" + tablefile + "'";
    goto abort;
  }

  m_hyperspace_ptr->close(handle);

  cb->response((char *)schemabuf.base);

  if (m_verbose) {
    HT_INFOF("Successfully fetched schema (length=%d) for table '%s'", strlen((char *)schemabuf.base), tablename);
  }

 abort:
  if (error != Error::OK) {
    if (m_verbose) {
      HT_ERRORF("%s '%s'", Error::get_text(error), errmsg.c_str());
    }
    cb->error(error, errmsg);
  }
  return;
}



/**
 *
 */
void Master::register_server(ResponseCallback *cb, const char *location, struct sockaddr_in &addr) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  RangeServerStatePtr rs_state;
  ServerMap::iterator iter;
  struct sockaddr_in alias;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
  HandleCallbackPtr lock_file_handler;
  uint32_t lock_status;
  LockSequencer lock_sequencer;
  String hsfname;

  HT_EXPECT((iter = m_server_map.find(location)) == m_server_map.end(), Error::FAILED_EXPECTATION);

  rs_state = new RangeServerState();
  rs_state->location = location;
  rs_state->addr = addr;

  if (!LocationCache::location_to_addr(location, alias)) {
    HT_ERRORF("Problem creating address from location '%s'", location);
    cb->error(Error::INVALID_METADATA, (String)"Unable to convert location '" + location + "' to address");
    return;
  }
  else {
    Comm *comm = m_conn_manager_ptr->get_comm();
    comm->set_alias(addr, alias);
  }

  hsfname = (String)"/hypertable/servers/" + location;

  lock_file_handler = new ServerLockFileHandler(rs_state, this, m_app_queue_ptr);

  if ((error = m_hyperspace_ptr->open(hsfname, oflags, lock_file_handler, &rs_state->hyperspace_handle)) != Error::OK) {
    HT_ERRORF("Problem opening discovered server file %s - %s", hsfname.c_str(), Error::get_text(error));
    cb->error(error, (String)"Problem opening discovered server file '" + hsfname + "'");
    return;
  }

  if ((error = m_hyperspace_ptr->try_lock(rs_state->hyperspace_handle, LOCK_MODE_EXCLUSIVE, &lock_status, &lock_sequencer)) != Error::OK) {
    HT_ERRORF("Problem attempting to obtain exclusive lock on server Hyperspace file '%s' - %s",
                 hsfname.c_str(), Error::get_text(error));
    cb->error(error, (String)"Problem attempting to obtain exclusive lock on server Hyperspace file '" + hsfname + "'");
    return;
  }

  if (lock_status == LOCK_STATUS_GRANTED) {
    HT_INFOF("Obtained lock on servers file %s, removing...", hsfname.c_str());
    if ((error = m_hyperspace_ptr->unlink(hsfname)) != Error::OK)
      HT_INFOF("Problem deleting Hyperspace file %s", hsfname.c_str());
    if ((error = m_hyperspace_ptr->close(rs_state->hyperspace_handle)) != Error::OK)
      HT_INFOF("Problem closing handle on deleting Hyperspace file %s", hsfname.c_str());
  }
  else
    m_server_map[rs_state->location] = rs_state;

  {
    String addr_str;
    HT_INFOF("Server Registered %s -> %s", location, InetAddr::string_format(addr_str, addr));
    cout << flush;
  }

  cb->response_ok();

  /**
   * TEMPORARY: Load root and second-level METADATA ranges
   */
  if (!m_initialized) {
    int error;
    TableIdentifier table;
    RangeSpec range;
    RangeServerClient rsc(m_conn_manager_ptr->get_comm(), 30);

    /**
     * Create METADATA table
     */
    {
      String errmsg;
      String metadata_schema_file = System::install_dir + "/conf/METADATA.xml";
      off_t schemalen;
      const char *schemastr = FileUtils::file_to_buffer(metadata_schema_file.c_str(), &schemalen);

      if ((error = create_table("METADATA", schemastr, errmsg)) != Error::OK) {
        HT_ERRORF("Problem creating METADATA table - %s", Error::get_text(error));
        return;
      }
    }

    /**
     * Open METADATA table
     */
    m_metadata_table_ptr = new Table(m_props_ptr, m_conn_manager_ptr->get_comm(), m_hyperspace_ptr, "METADATA");

    m_metadata_table_ptr->get_identifier(&table);
    table.name = "METADATA";

    /**
     * Load root METADATA range
     */
    range.start_row = 0;
    range.end_row = Key::END_ROOT_ROW;

    try {
      RangeState range_state;
      range_state.soft_limit = m_max_range_bytes;
      rsc.load_range(alias, table, range, 0, range_state, 0);
    }
    catch (Exception &e) {
      String addr_str;
      HT_ERRORF("Problem issuing 'load range' command for %s[..%s] at server %s - %s",
                table.name, range.end_row, InetAddr::string_format(addr_str, alias), Error::get_text(e.code()));
    }


    /**
     * Write METADATA entry for second-level METADATA range
     */

    TableMutatorPtr mutator_ptr;
    KeySpec key;
    String metadata_key_str;

    mutator_ptr = m_metadata_table_ptr->create_mutator();

    metadata_key_str = String("0:") + Key::END_ROW_MARKER;
    key.row = metadata_key_str.c_str();
    key.row_len = metadata_key_str.length();
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;

    try {
      key.column_family = "StartRow";
      mutator_ptr->set(0, key, (uint8_t *)Key::END_ROOT_ROW, strlen(Key::END_ROOT_ROW));
      mutator_ptr->flush();
    }
    catch (Hypertable::Exception &e) {
      // TODO: propagate exception
      HT_ERRORF("METADATA update error (row_key = %s) - %s : %s", metadata_key_str.c_str(), e.what(), Error::get_text(e.code()));
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
      rsc.load_range(alias, table, range, 0, range_state, 0);
    }
    catch (Exception &e) {
      String addr_str;
      HT_ERRORF("Problem issuing 'load range' command for %s[..%s] at server %s - %s",
                table.name, range.end_row, InetAddr::string_format(addr_str, alias), Error::get_text(e.code()));
    }

    m_initialized = true;
  }

}

/**
 * TEMPORARY: Just turns around and assigns new range to caller
 *
 * NOTE: this call can't be protected by a mutex because it can cause the
 * whole system to wedge under certain situations
 */
void Master::report_split(ResponseCallback *cb, TableIdentifier &table, RangeSpec &range, const char *transfer_log_dir, uint64_t soft_limit) {
  struct sockaddr_in addr;
  RangeServerClient rsc(m_conn_manager_ptr->get_comm(), 30);

  HT_INFOF("Entering report_split for %s[%s:%s].", table.name, range.start_row, range.end_row);

  cb->response_ok();

  {
    boost::mutex::scoped_lock lock(m_mutex);
    if (m_server_map_iter == m_server_map.end())
      m_server_map_iter = m_server_map.begin();
    assert(m_server_map_iter != m_server_map.end());
    memcpy(&addr, &((*m_server_map_iter).second->addr), sizeof(struct sockaddr_in));
    HT_INFOF("Assigning newly reported range %s[%s:%s] to %s", table.name, range.start_row, range.end_row, (*m_server_map_iter).first.c_str());
    m_server_map_iter++;
  }

  //cb->get_address(addr);

  try {
    RangeState range_state;
    range_state.soft_limit = soft_limit;
    rsc.load_range(addr, table, range, transfer_log_dir, range_state, 0);
    HT_INFOF("report_split for %s[%s:%s] successful.", table.name, range.start_row, range.end_row);
  }
  catch (Exception &e) {
    String addr_str;
    HT_ERRORF("Problem issuing 'load range' command for %s[%s:%s] at server %s - %s",
              table.name, range.start_row, range.end_row, InetAddr::string_format(addr_str, addr), Error::get_text(e.code()));
  }

}

void Master::drop_table(ResponseCallback *cb, const char *table_name, bool if_exists) {
  int error = Error::OK;
  int saved_error = Error::OK;
  String err_msg;
  String table_file = (String)"/hypertable/tables/" + table_name;
  DynamicBuffer value_buf(0);
  int ival;
  HandleCallbackPtr null_handle_callback;
  uint64_t handle;
  String table_name_str = table_name;

  /**
   * Create table file
   */
  if ((error = m_hyperspace_ptr->open(table_file.c_str(), OPEN_FLAG_READ, null_handle_callback, &handle)) != Error::OK) {
    if (if_exists && error == Error::HYPERSPACE_BAD_PATHNAME)
      cb->response_ok();
    else
      cb->error(error, (String)"Problem opening file '" + table_file + "'");
    return;
  }

  /**
   *
   */
  if ((error = m_hyperspace_ptr->attr_get(handle, "table_id", value_buf)) != Error::OK) {
    HT_ERRORF("Problem getting attribute 'table_id' from file '%s' - %s", table_file.c_str(), Error::get_text(error));
    cb->error(error, (String)"Problem reading attribute 'table_id' from file '" + table_file + "'");
    return;
  }

  /**
   *
   */
  if ((error = m_hyperspace_ptr->close(handle)) != Error::OK) {
    HT_ERRORF("Problem closing hyperspace handle %lld - %s", (long long int)handle, Error::get_text(error));
  }

  assert(value_buf.fill() == sizeof(int32_t));

  memcpy(&ival, value_buf.base, sizeof(int32_t));

  {
    char start_row[16];
    char end_row[16];
    TableScannerPtr scanner_ptr;
    ScanSpec scan_spec;
    Cell cell;
    String location_str;
    std::set<String> unique_locations;
    TableIdentifier table;

    table.name = table_name_str.c_str();
    table.id = ival;
    table.generation = 0;

    sprintf(start_row, "%d:", ival);
    sprintf(end_row, "%d:%s", ival, Key::END_ROW_MARKER);

    scan_spec.row_limit = 0;
    scan_spec.max_versions = 1;
    scan_spec.columns.clear();
    scan_spec.columns.push_back("Location");
    scan_spec.start_row = start_row;
    scan_spec.start_row_inclusive = true;
    scan_spec.end_row = end_row;
    scan_spec.end_row_inclusive = true;
    scan_spec.interval.first = 0;
    scan_spec.interval.second = 0;
    scan_spec.return_deletes=false;

    scanner_ptr = m_metadata_table_ptr->create_scanner(scan_spec);

    while (scanner_ptr->next(cell)) {
      location_str = String((const char *)cell.value, cell.value_len);
      boost::trim(location_str);
      if (location_str != "" && location_str != "!")
        unique_locations.insert(location_str);
    }

    if (!unique_locations.empty()) {
      boost::mutex::scoped_lock lock(m_mutex);
      DropTableDispatchHandler sync_handler(table, m_conn_manager_ptr->get_comm(), 30);
      RangeServerStatePtr state_ptr;
      ServerMap::iterator iter;

      for (std::set<String>::iterator loc_iter = unique_locations.begin(); loc_iter != unique_locations.end(); loc_iter++) {
        if ((iter = m_server_map.find(*loc_iter)) != m_server_map.end()) {
          sync_handler.add((*iter).second->addr);
        }
        else {
          saved_error = Error::RANGESERVER_UNAVAILABLE;
          err_msg = *loc_iter;
        }
      }

      if (!sync_handler.wait_for_completion()) {
        std::vector<DropTableDispatchHandler::ErrorResult> errors;
        sync_handler.get_errors(errors);
        for (size_t i=0; i<errors.size(); i++) {
          HT_WARNF("drop table error - %s - %s", errors[i].msg.c_str(), Error::get_text(errors[i].error));
        }
        cb->error(errors[0].error, errors[0].msg);
        return;
      }
    }

  }

  if (saved_error != Error::OK) {
    HT_ERRORF("DROP TABLE failed '%s' - %s", err_msg.c_str(), Error::get_text(saved_error));
    cb->error(saved_error, err_msg);
    return;
  }
  else if ((error = m_hyperspace_ptr->unlink(table_file.c_str())) != Error::OK) {
    HT_ERRORF("Problem removing hyperspace file - %s", Error::get_text(error));
    cb->error(error, (String)"Problem removing file '" + table_file + "'");
    return;
  }

  HT_INFOF("DROP TABLE '%s' id=%d success", table_name_str.c_str(), ival);
  cb->response_ok();
  cout << flush;
}


int
Master::create_table(const char *tablename, const char *schemastr,
                     String &errmsg) {
  int error = Error::OK;
  String finalschema = "";
  String tablefile = (String)"/hypertable/tables/" + tablename;
  string table_basedir;
  string agdir;
  Schema *schema = 0;
  list<Schema::AccessGroup *> *aglist;
  bool exists;
  HandleCallbackPtr null_handle_callback;
  uint64_t handle;
  uint32_t table_id;

  m_hyperspace_ptr->set_silent_flag(true);

  /**
   * Check for table existence
   */
  if ((error = m_hyperspace_ptr->exists(tablefile, &exists)) != Error::OK) {
    errmsg = (String)"Problem checking for existence of table file '" + tablefile + "'";
    goto abort;
  }
  if (exists) {
    errmsg = tablename;
    error = Error::MASTER_TABLE_EXISTS;
    goto abort;
  }

  /**
   *  Parse Schema and assign Generation number and Column ids
   */
  schema = Schema::new_instance(schemastr, strlen(schemastr));
  if (!schema->is_valid()) {
    errmsg = schema->get_error_string();
    error = Error::MASTER_BAD_SCHEMA;
    goto abort;
  }
  schema->assign_ids();
  schema->render(finalschema);

  /**
   * Create table file
   */
  if ((error = m_hyperspace_ptr->open(tablefile, OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE, null_handle_callback, &handle)) != Error::OK) {
    errmsg = "Unable to create Hyperspace table file '" + tablefile + "'";
    goto abort;
  }

  /**
   * Write 'table_id' attribute of table file and 'last_table_id' attribute of /hypertable/master
   */
  {
    if (!strcmp(tablename, "METADATA"))
      table_id = 0;
    else {
      table_id = (uint32_t)atomic_inc_return(&m_last_table_id);
      if ((error = m_hyperspace_ptr->attr_set(m_master_file_handle, "last_table_id", &table_id, sizeof(int32_t))) != Error::OK) {
        errmsg = (String)"Problem setting attribute 'last_table_id' of file /hypertable/master - " + Error::get_text(error);
        goto abort;
      }
    }

    if ((error = m_hyperspace_ptr->attr_set(handle, "table_id", &table_id, sizeof(int32_t))) != Error::OK) {
      errmsg = (String)"Problem setting attribute 'table_id' of file " + tablefile + " - " + Error::get_text(error);
      goto abort;
    }
  }


  /**
   * Write schema attribute
   */
  if ((error = m_hyperspace_ptr->attr_set(handle, "schema", finalschema.c_str(), strlen(finalschema.c_str()))) != Error::OK) {
    errmsg = (String)"Problem creating attribute 'schema' for table file '" + tablefile + "'";
    goto abort;
  }

  m_hyperspace_ptr->close(handle);

  /**
   * Create /hypertable/tables/&lt;table&gt;/&lt;accessGroup&gt; directories for this table in HDFS
   */
  table_basedir = (string)"/hypertable/tables/" + tablename + "/";
  aglist = schema->get_access_group_list();

  for (list<Schema::AccessGroup *>::iterator ag_it = aglist->begin();
      ag_it != aglist->end(); ag_it++) {
    agdir = table_basedir + (*ag_it)->name;
    m_dfs_client->mkdirs(agdir);
  }

  /**
   * Write METADATA entry, single range covering entire table '\\0' to 0xff 0xff
   */
  if (table_id != 0) {
    TableMutatorPtr mutator_ptr;
    KeySpec key;
    String metadata_key_str;
    struct sockaddr_in addr;

    mutator_ptr = m_metadata_table_ptr->create_mutator();

    metadata_key_str = String("") + table_id + ":" + Key::END_ROW_MARKER;
    key.row = metadata_key_str.c_str();
    key.row_len = metadata_key_str.length();
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;

    try {
      key.column_family = "StartRow";
      mutator_ptr->set(0, key, 0, 0);
      mutator_ptr->flush();
    }
    catch (Hypertable::Exception &e) {
      errmsg = (String)"Problem updating METADATA with new table info (row key = " + metadata_key_str.c_str() + ") - " + e.what();
      goto abort;
    }

    /**
     * TEMPORARY:  ask the one Range Server that we know about to load the range
     */

    TableIdentifier table;
    RangeSpec range;
    uint64_t soft_limit;
    RangeServerClient rsc(m_conn_manager_ptr->get_comm(), 30);

    table.name = tablename;
    table.id = table_id;
    table.generation = schema->get_generation();

    range.start_row = 0;
    range.end_row = Key::END_ROW_MARKER;

    {
      boost::mutex::scoped_lock lock(m_mutex);
      if (m_server_map_iter == m_server_map.end())
        m_server_map_iter = m_server_map.begin();
      assert(m_server_map_iter != m_server_map.end());
      memcpy(&addr, &((*m_server_map_iter).second->addr), sizeof(struct sockaddr_in));
      HT_INFOF("Assigning first range %s[%s:%s] to %s", table.name, range.start_row, range.end_row, (*m_server_map_iter).first.c_str());
      m_server_map_iter++;
      soft_limit = m_max_range_bytes / std::min(64, (int)m_server_map.size()*2);
    }

    try {
      RangeState range_state;
      range_state.soft_limit = soft_limit;
      rsc.load_range(addr, table, range, 0, range_state);
    }
    catch (Exception &e) {
      String addr_str;
      HT_ERRORF("Problem issuing 'load range' command for %s[..%s] at server %s - %s",
                table.name, range.end_row, InetAddr::string_format(addr_str, addr), Error::get_text(e.code()));
    }
  }

  if (m_verbose) {
    HT_INFOF("Successfully created table '%s' ID=%d", tablename, table_id);
  }

abort:
  m_hyperspace_ptr->set_silent_flag(false);
  delete schema;
  if (error != Error::OK) {
    if (m_verbose) {
      HT_ERRORF("%s '%s'", Error::get_text(error), errmsg.c_str());
    }
  }
  return error;
}


/**
 * PRIVATE Methods
 */

bool Master::initialize() {
  int error;
  bool exists;
  uint64_t handle;
  HandleCallbackPtr null_handle_callback;

  if ((error = m_hyperspace_ptr->exists("/hypertable/master", &exists)) == Error::OK && exists &&
      (error = m_hyperspace_ptr->exists("/hypertable/servers", &exists)) == Error::OK && exists)
    return true;

  if (!create_hyperspace_dir("/hypertable"))
    return false;

  if (!create_hyperspace_dir("/hypertable/servers"))
    return false;

  if (!create_hyperspace_dir("/hypertable/tables"))
    return false;

  /**
   * Create /hypertable/master
   */
  if ((error = m_hyperspace_ptr->open("/hypertable/master", OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE, null_handle_callback, &handle)) != Error::OK) {
    HT_ERRORF("Unable to open Hyperspace file '/hypertable/master' (%s)", Error::get_text(error));
    return false;
  }
  m_master_file_handle = handle;

  /**
   * Initialize last_table_id to 0
   */
  uint32_t table_id = 0;
  if ((error = m_hyperspace_ptr->attr_set(m_master_file_handle, "last_table_id", &table_id, sizeof(int32_t))) != Error::OK) {
    HT_ERRORF("Problem setting attribute 'last_table_id' of file /hypertable/master - %s", Error::get_text(error));
    return false;
  }

  m_hyperspace_ptr->close(handle);
  m_master_file_handle = 0;

  /**
   *  Create /hypertable/root
   */
  if ((error = m_hyperspace_ptr->open("/hypertable/root", OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE, null_handle_callback, &handle)) != Error::OK) {
    HT_ERRORF("Unable to open Hyperspace file '/hypertable/root' (%s)", Error::get_text(error));
    return false;
  }
  m_hyperspace_ptr->close(handle);

  HT_INFO("Successfully Initialized Hypertable.");

  return true;
}


/**
 *
 */
void Master::scan_servers_directory() {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  HandleCallbackPtr lock_file_handler;
  std::vector<struct DirEntry> listing;
  uint32_t lock_status;
  LockSequencer lock_sequencer;
  RangeServerStatePtr rs_state;
  uint32_t oflags;
  String hsfname;

  /**
   * Open /hyperspace/servers directory and scan for range servers
   */
  m_servers_dir_callback_ptr = new ServersDirectoryHandler(this, m_app_queue_ptr);

  if ((error = m_hyperspace_ptr->open("/hypertable/servers", OPEN_FLAG_READ, m_servers_dir_callback_ptr, &m_servers_dir_handle)) != Error::OK) {
    HT_ERRORF("Unable to open Hyperspace directory '/hypertable/servers' (%s)  Try re-running with --initialize",
                 Error::get_text(error));
    exit(1);
  }

  if ((error = m_hyperspace_ptr->readdir(m_servers_dir_handle, listing)) != Error::OK) {
    HT_ERRORF("Problem scanning Hyperspace directory '/hypertable/servers' - %s", Error::get_text(error));
    exit(1);
  }

  oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;

  for (size_t i=0; i<listing.size(); i++) {

    rs_state = new RangeServerState();
    rs_state->location = listing[i].name;

    hsfname = (String)"/hypertable/servers/" + listing[i].name;

    lock_file_handler = new ServerLockFileHandler(rs_state, this, m_app_queue_ptr);

    if ((error = m_hyperspace_ptr->open(hsfname, oflags, lock_file_handler, &rs_state->hyperspace_handle)) != Error::OK) {
      HT_ERRORF("Problem opening discovered server file %s - %s", hsfname.c_str(), Error::get_text(error));
      continue;
    }

    if ((error = m_hyperspace_ptr->try_lock(rs_state->hyperspace_handle, LOCK_MODE_EXCLUSIVE, &lock_status, &lock_sequencer)) != Error::OK) {
      HT_ERRORF("Problem obtaining exclusive lock on master Hyperspace file '/hypertable/master' - %s", Error::get_text(error));
      continue;
    }

    if (lock_status == LOCK_STATUS_GRANTED) {
      HT_INFOF("Obtained lock on servers file %s, removing...", hsfname.c_str());
      if ((error = m_hyperspace_ptr->unlink(hsfname)) != Error::OK)
        HT_INFOF("Problem deleting Hyperspace file %s", hsfname.c_str());
      if ((error = m_hyperspace_ptr->close(rs_state->hyperspace_handle)) != Error::OK)
        HT_INFOF("Problem closing handle on deleting Hyperspace file %s", hsfname.c_str());
    }
    else {
      m_server_map[rs_state->location] = rs_state;
    }
  }

}


/**
 *
 */
bool Master::create_hyperspace_dir(const String &dir) {
  int error;
  bool exists;

  /**
   * Check for existence
   */
  if ((error = m_hyperspace_ptr->exists(dir, &exists)) != Error::OK) {
    HT_ERRORF("Problem checking for existence of directory '%s' - %s", dir.c_str(), Error::get_text(error));
    return false;
  }

  if (exists)
    return true;

  if ((error = m_hyperspace_ptr->mkdir(dir)) != Error::OK) {
    HT_ERRORF("Problem creating directory '%s' - %s", dir.c_str(), Error::get_text(error));
    return false;
  }

  return true;
}

void Master::join() {
  m_app_queue_ptr->join();
  m_threads.join_all();
}

} // namespace Hypertable
