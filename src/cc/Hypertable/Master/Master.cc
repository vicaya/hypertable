/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <algorithm>
#include <string>

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

Master::Master(ConnectionManagerPtr &connManagerPtr, PropertiesPtr &props_ptr, ApplicationQueuePtr &appQueuePtr) : m_props_ptr(props_ptr), m_conn_manager_ptr(connManagerPtr), m_app_queue_ptr(appQueuePtr), m_verbose(false), m_dfs_client(0), m_initialized(false) {
  int error;
  Client *dfsClient;
  uint16_t port;

  m_server_map_iter = m_server_map.begin();
  
  m_hyperspace_ptr = new Hyperspace::Session(connManagerPtr->get_comm(), props_ptr, &m_hyperspace_session_handler);

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
  dfsClient = new DfsBroker::Client(connManagerPtr, props_ptr);

  if (m_verbose) {
    cout << "DfsBroker.Host=" << props_ptr->get("DfsBroker.Host", "") << endl;
    cout << "DfsBroker.Port=" << props_ptr->get("DfsBroker.Port", "") << endl;
    cout << "DfsBroker.Timeout=" << props_ptr->get("DfsBroker.Timeout", "") << endl;
  }

  if (!dfsClient->wait_for_connection(30)) {
    HT_ERROR("Unable to connect to DFS Broker, exiting...");
    exit(1);
  }

  m_dfs_client = dfsClient;

  atomic_set(&m_last_table_id, 0);

  if (!initialize())
    exit(1);

  /* Read Last Table ID */
  {
    DynamicBuffer valueBuf(0);
    HandleCallbackPtr nullHandleCallback;
    int ival;
    uint32_t lockStatus;
    uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;

    if ((error = m_hyperspace_ptr->open("/hypertable/master", oflags, nullHandleCallback, &m_master_file_handle)) != Error::OK) {
      HT_ERRORF("Unable to open Hyperspace file '/hypertable/master' (%s)  Try re-running with --initialize",
		   Error::get_text(error));
      exit(1);
    }

    if ((error = m_hyperspace_ptr->try_lock(m_master_file_handle, LOCK_MODE_EXCLUSIVE, &lockStatus, &m_master_file_sequencer)) != Error::OK) {
      HT_ERRORF("Problem obtaining exclusive lock on master Hyperspace file '/hypertable/master' - %s", Error::get_text(error));
      exit(1);
    }
    
    if (lockStatus != LOCK_STATUS_GRANTED) {
      HT_ERROR("Unable to obtain lock on '/hypertable/master' - conflict");
      exit(1);
    }

    // Write master location in 'address' attribute, format is IP:port
    {
      std::string hostStr, addrStr;
      struct hostent *he;
      InetAddr::get_hostname(hostStr);
      if ((he = gethostbyname(hostStr.c_str())) == 0) {
	HT_ERRORF("Problem obtaining address for hostname '%s'", hostStr.c_str());
	exit(1);
      }
      addrStr = std::string(inet_ntoa(*(struct in_addr *)*he->h_addr_list)) + ":" + (int)port;
      if ((error = m_hyperspace_ptr->attr_set(m_master_file_handle, "address", addrStr.c_str(), strlen(addrStr.c_str()))) != Error::OK) {
	HT_ERRORF("Problem setting attribute 'address' of hyperspace file /hypertable/master - %s", Error::get_text(error));
	exit(1);
      }
    }

    if ((error = m_hyperspace_ptr->attr_get(m_master_file_handle, "last_table_id", valueBuf)) != Error::OK) {
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
      assert(valueBuf.fill() == sizeof(int32_t));
      memcpy(&ival, valueBuf.buf, sizeof(int32_t));
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
void Master::server_joined(std::string &location) {
  HT_INFOF("Server Joined (%s)", location.c_str());
  cout << flush;
}



/**
 * 
 */
void Master::server_left(std::string &location) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  uint32_t lockStatus;
  struct LockSequencerT lockSequencer;
  ServerMapT::iterator iter = m_server_map.find(location);
  std::string hsFilename = (std::string)"/hypertable/servers/" + location;

  if (iter == m_server_map.end()) {
    HT_WARNF("Server (%s) not found in map", location.c_str());
    return;
  }

  // if we're about to delete the item pointing to the server map iterator, then advance the iterator
  if (iter == m_server_map_iter)
    m_server_map_iter++;

  if ((error = m_hyperspace_ptr->try_lock((*iter).second->hyperspaceHandle, LOCK_MODE_EXCLUSIVE, &lockStatus, &lockSequencer)) != Error::OK) {
    HT_ERRORF("Problem obtaining exclusive lock on master Hyperspace file '/hypertable/master' - %s", Error::get_text(error));
    return;
  }

  if (lockStatus != LOCK_STATUS_GRANTED) {
    HT_INFOF("Unable to obtain lock on server file %s, ignoring...", location.c_str());
    return;
  }

  m_hyperspace_ptr->unlink(hsFilename);
  m_hyperspace_ptr->close((*iter).second->hyperspaceHandle);
  m_server_map.erase(iter);

  HT_INFOF("RangeServer lost it's lock on file %s, deleting ...", hsFilename.c_str());
  cout << flush;

  /**
   *  Do (or schedule) tablet re-assignment here
   */
}



/**
 * 
 */
void Master::create_table(ResponseCallback *cb, const char *tableName, const char *schemaString) {
  int error;
  std::string errMsg;
  std::string table_name_str = tableName;

#if defined(__APPLE__)
  boost::to_upper(table_name_str);
#endif  

  if ((error = create_table(table_name_str.c_str(), schemaString, errMsg)) != Error::OK)
    cb->error(error, errMsg);
  else
    cb->response_ok();
}


/**
 *
 */
void Master::get_schema(ResponseCallbackGetSchema *cb, const char *tableName) {
  int error = Error::OK;
  std::string tableFile = (std::string)"/hypertable/tables/" + tableName;
  std::string errMsg;
  DynamicBuffer schemaBuf(0);
  uint64_t handle;
  bool exists;
  HandleCallbackPtr nullHandleCallback;

  /**
   * Check for table existence
   */
  if ((error = m_hyperspace_ptr->exists(tableFile, &exists)) != Error::OK) {
    errMsg = (std::string)"Problem checking for existence of table '" + tableName + "' - " + Error::get_text(error);
    goto abort;
  }

  /**
   * Open table file
   */
  if ((error = m_hyperspace_ptr->open(tableFile, OPEN_FLAG_READ, nullHandleCallback, &handle)) != Error::OK) {
    errMsg = "Unable to open Hyperspace table file '" + tableFile + "' (" + Error::get_text(error) + ")";
    goto abort;
  }

  /**
   * Get schema attribute
   */
  if ((error = m_hyperspace_ptr->attr_get(handle, "schema", schemaBuf)) != Error::OK) {
    errMsg = "Problem getting attribute 'schema' for table file '" + tableFile + "'";
    goto abort;
  }

  m_hyperspace_ptr->close(handle);
  
  cb->response((char *)schemaBuf.buf);

  if (m_verbose) {
    HT_INFOF("Successfully fetched schema (length=%d) for table '%s'", strlen((char *)schemaBuf.buf), tableName);
  }

 abort:
  if (error != Error::OK) {
    if (m_verbose) {
      HT_ERRORF("%s '%s'", Error::get_text(error), errMsg.c_str());
    }
    cb->error(error, errMsg);
  }
  return;
}



/**
 * 
 */
void Master::register_server(ResponseCallback *cb, const char *location, struct sockaddr_in &addr) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  RangeServerStatePtr statePtr;
  ServerMapT::iterator iter;
  struct sockaddr_in alias;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
  HandleCallbackPtr lockFileHandler;
  uint32_t lockStatus;
  struct LockSequencerT lockSequencer;
  std::string hsFilename;

  HT_EXPECT((iter = m_server_map.find(location)) == m_server_map.end(), Error::FAILED_EXPECTATION);

  statePtr = new RangeServerState();
  statePtr->location = location;
  statePtr->addr = addr;

  if (!LocationCache::location_to_addr(location, alias)) {
    HT_ERRORF("Problem creating address from location '%s'", location);
    cb->error(Error::INVALID_METADATA, (std::string)"Unable to convert location '" + location + "' to address");
    return;
  }
  else {
    Comm *comm = m_conn_manager_ptr->get_comm();
    comm->set_alias(addr, alias);
  }

  hsFilename = (std::string)"/hypertable/servers/" + location;

  lockFileHandler = new ServerLockFileHandler(statePtr, this, m_app_queue_ptr);

  if ((error = m_hyperspace_ptr->open(hsFilename, oflags, lockFileHandler, &statePtr->hyperspaceHandle)) != Error::OK) {
    HT_ERRORF("Problem opening discovered server file %s - %s", hsFilename.c_str(), Error::get_text(error));
    cb->error(error, (std::string)"Problem opening discovered server file '" + hsFilename + "'");
    return;
  }

  if ((error = m_hyperspace_ptr->try_lock(statePtr->hyperspaceHandle, LOCK_MODE_EXCLUSIVE, &lockStatus, &lockSequencer)) != Error::OK) {
    HT_ERRORF("Problem attempting to obtain exclusive lock on server Hyperspace file '%s' - %s",
		 hsFilename.c_str(), Error::get_text(error));
    cb->error(error, (std::string)"Problem attempting to obtain exclusive lock on server Hyperspace file '" + hsFilename + "'");
    return;
  }

  if (lockStatus == LOCK_STATUS_GRANTED) {
    HT_INFOF("Obtained lock on servers file %s, removing...", hsFilename.c_str());
    if ((error = m_hyperspace_ptr->unlink(hsFilename)) != Error::OK)
      HT_INFOF("Problem deleting Hyperspace file %s", hsFilename.c_str());
    if ((error = m_hyperspace_ptr->close(statePtr->hyperspaceHandle)) != Error::OK)
      HT_INFOF("Problem closing handle on deleting Hyperspace file %s", hsFilename.c_str());
  }
  else
    m_server_map[statePtr->location] = statePtr;  

  {
    std::string addrStr;
    HT_INFOF("Server Registered %s -> %s", location, InetAddr::string_format(addrStr, addr));
    cout << flush;
  }

  cb->response_ok();

  /**
   * TEMPORARY: Load root and second-level METADATA ranges
   */
  if (!m_initialized) {
    int error;
    TableIdentifierT table;
    RangeT range;
    RangeServerClient rsc(m_conn_manager_ptr->get_comm(), 30);

    /**
     * Create METADATA table
     */
    {
      std::string errMsg;
      std::string metadataSchemaFile = System::installDir + "/conf/METADATA.xml";
      off_t schemaLen;
      const char *schemaStr = FileUtils::file_to_buffer(metadataSchemaFile.c_str(), &schemaLen);

      if ((error = create_table("METADATA", schemaStr, errMsg)) != Error::OK) {
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
    range.startRow = 0;
    range.endRow = Key::END_ROOT_ROW;

    if ((error = rsc.load_range(alias, table, range, m_max_range_bytes, 0)) != Error::OK) {
      std::string addrStr;
      HT_ERRORF("Problem issuing 'load range' command for %s[..%s] at server %s",
		   table.name, range.endRow, InetAddr::string_format(addrStr, alias));
    }


    /**
     * Write METADATA entry for second-level METADATA range
     */

    TableMutatorPtr mutator_ptr;
    KeySpec key;
    std::string metadata_key_str;

    if ((error = m_metadata_table_ptr->create_mutator(mutator_ptr)) != Error::OK) {
      // TODO: throw exception
      HT_ERROR("Problem creating mutator on METADATA table");
      exit(1);
    }

    metadata_key_str = std::string("0:") + Key::END_ROW_MARKER;
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
    range.startRow = Key::END_ROOT_ROW;
    range.endRow = Key::END_ROW_MARKER;

    if ((error = rsc.load_range(alias, table, range, m_max_range_bytes, 0)) != Error::OK) {
      std::string addrStr;
      HT_ERRORF("Problem issuing 'load range' command for %s[..%s] at server %s",
		   table.name, range.endRow, InetAddr::string_format(addrStr, alias));
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
void Master::report_split(ResponseCallback *cb, TableIdentifierT &table, RangeT &range, uint64_t soft_limit) {
  int error;
  struct sockaddr_in addr;
  RangeServerClient rsc(m_conn_manager_ptr->get_comm(), 30);

  HT_INFOF("Entering report_split for %s[%s:%s].", table.name, range.startRow, range.endRow);

  cb->response_ok();

  {
    boost::mutex::scoped_lock lock(m_mutex);
    if (m_server_map_iter == m_server_map.end())
      m_server_map_iter = m_server_map.begin();
    assert(m_server_map_iter != m_server_map.end());
    memcpy(&addr, &((*m_server_map_iter).second->addr), sizeof(struct sockaddr_in));
    HT_INFOF("Assigning newly reported range %s[%s:%s] to %s", table.name, range.startRow, range.endRow, (*m_server_map_iter).first.c_str());
    m_server_map_iter++;
  }

  //cb->get_address(addr);

  if ((error = rsc.load_range(addr, table, range, soft_limit, 0)) != Error::OK) {
    std::string addrStr;
    HT_ERRORF("Problem issuing 'load range' command for %s[%s:%s] at server %s",
                 table.name, range.startRow, range.endRow, InetAddr::string_format(addrStr, addr));
  }
  else
    HT_INFOF("report_split for %s[%s:%s] successful.", table.name, range.startRow, range.endRow);

}

void Master::drop_table(ResponseCallback *cb, const char *table_name, bool if_exists) {
  int error = Error::OK;
  int saved_error = Error::OK;
  std::string err_msg;
  std::string table_file = (std::string)"/hypertable/tables/" + table_name;
  DynamicBuffer value_buf(0);
  int ival;
  HandleCallbackPtr nullHandleCallback;
  uint64_t handle;
  std::string table_name_str = table_name;

#if defined(__APPLE__)
  boost::to_upper(table_name_str);
#endif

  /**
   * Create table file
   */
  if ((error = m_hyperspace_ptr->open(table_file.c_str(), OPEN_FLAG_READ, nullHandleCallback, &handle)) != Error::OK) {
    if (if_exists && error == Error::HYPERSPACE_BAD_PATHNAME)
      cb->response_ok();
    else
      cb->error(error, (std::string)"Problem opening file '" + table_file + "'");
    return;
  }

  /**
   * 
   */
  if ((error = m_hyperspace_ptr->attr_get(handle, "table_id", value_buf)) != Error::OK) {
    HT_ERRORF("Problem getting attribute 'table_id' from file '%s' - %s", table_file.c_str(), Error::get_text(error));
    cb->error(error, (std::string)"Problem reading attribute 'table_id' from file '" + table_file + "'");
    return;
  }

  /**
   * 
   */
  if ((error = m_hyperspace_ptr->close(handle)) != Error::OK) {
    HT_ERRORF("Problem closing hyperspace handle %lld - %s", (long long int)handle, Error::get_text(error));
  }

  assert(value_buf.fill() == sizeof(int32_t));

  memcpy(&ival, value_buf.buf, sizeof(int32_t));

  {
    char start_row[16];
    char end_row[16];
    TableScannerPtr scanner_ptr;
    ScanSpecificationT scan_spec;
    CellT cell;
    std::string location_str;
    std::set<std::string> unique_locations;

    sprintf(start_row, "%d:", ival);
    sprintf(end_row, "%d:%s", ival, Key::END_ROW_MARKER);

    scan_spec.rowLimit = 0;
    scan_spec.max_versions = 1;
    scan_spec.columns.clear();
    scan_spec.columns.push_back("Location");
    scan_spec.startRow = start_row;
    scan_spec.startRowInclusive = true;
    scan_spec.endRow = end_row;
    scan_spec.endRowInclusive = true;
    scan_spec.interval.first = 0;
    scan_spec.interval.second = 0;
    scan_spec.return_deletes=false;

    if ((error = m_metadata_table_ptr->create_scanner(scan_spec, scanner_ptr)) != Error::OK) {
      HT_ERRORF("Problem creating scanner on METADATA table - %s", Error::get_text(error));
      cb->error(error, "Problem creating scanner on METADATA table");
      return;
    }
    else {
      while (scanner_ptr->next(cell)) {
        location_str = std::string((const char *)cell.value, cell.value_len);
        boost::trim(location_str);
        if (location_str != "" && location_str != "!")
          unique_locations.insert(location_str);
      }
    }

    if (!unique_locations.empty()) {
      boost::mutex::scoped_lock lock(m_mutex);
      DropTableDispatchHandler sync_handler(table_name_str, m_conn_manager_ptr->get_comm(), 30);
      RangeServerStatePtr state_ptr;
      ServerMapT::iterator iter;

      for (std::set<std::string>::iterator loc_iter = unique_locations.begin(); loc_iter != unique_locations.end(); loc_iter++) {
        if ((iter = m_server_map.find(*loc_iter)) != m_server_map.end()) {
          sync_handler.add( (*iter).second->addr );
        }
        else {
          saved_error = Error::RANGESERVER_UNAVAILABLE;
          err_msg = *loc_iter;
        }
      }

      if (!sync_handler.wait_for_completion()) {
        std::vector<DropTableDispatchHandler::ErrorResultT> errors;
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
    HT_ERRORF("DROP TABLE failed '%s' - %s", err_msg.c_str(), Error::get_text(error));
    cb->error(saved_error, err_msg);
    return;
  }
  else if ((error = m_hyperspace_ptr->unlink(table_file.c_str())) != Error::OK) {
    HT_ERRORF("Problem removing hyperspace file - %s", Error::get_text(error));
    cb->error(error, (std::string)"Problem removing file '" + table_file + "'");
    return;
  }

  HT_INFOF("DROP TABLE '%s' id=%d success", table_name_str.c_str(), ival);
  cb->response_ok();
  cout << flush;
}


int Master::create_table(const char *tableName, const char *schemaString, std::string &errMsg) {
  int error = Error::OK;
  std::string finalSchema = "";
  std::string tableFile = (std::string)"/hypertable/tables/" + tableName;
  string tableBaseDir;
  string lgDir;
  Schema *schema = 0;
  list<Schema::AccessGroup *> *lgList;
  bool exists;
  HandleCallbackPtr nullHandleCallback;
  uint64_t handle;
  uint32_t table_id;

  m_hyperspace_ptr->set_silent_flag(true);

  /**
   * Check for table existence
   */
  if ((error = m_hyperspace_ptr->exists(tableFile, &exists)) != Error::OK) {
    errMsg = (std::string)"Problem checking for existence of table file '" + tableFile + "'";
    goto abort;
  }
  if (exists) {
    errMsg = tableName;
    error = Error::MASTER_TABLE_EXISTS;
    goto abort;
  }

  /**
   *  Parse Schema and assign Generation number and Column ids
   */
  schema = Schema::new_instance(schemaString, strlen(schemaString));
  if (!schema->is_valid()) {
    errMsg = schema->get_error_string();
    error = Error::MASTER_BAD_SCHEMA;
    goto abort;
  }
  schema->assign_ids();
  schema->render(finalSchema);

  /**
   * Create table file
   */
  if ((error = m_hyperspace_ptr->open(tableFile, OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE, nullHandleCallback, &handle)) != Error::OK) {
    errMsg = "Unable to create Hyperspace table file '" + tableFile + "' (" + Error::get_text(error) + ")";
    goto abort;
  }

  /**
   * Write 'table_id' attribute of table file and 'last_table_id' attribute of /hypertable/master
   */
  {
    if (!strcmp(tableName, "METADATA"))
      table_id = 0;
    else {
      table_id = (uint32_t)atomic_inc_return(&m_last_table_id);
      if ((error = m_hyperspace_ptr->attr_set(m_master_file_handle, "last_table_id", &table_id, sizeof(int32_t))) != Error::OK) {
        errMsg = (std::string)"Problem setting attribute 'last_table_id' of file /hypertable/master - " + Error::get_text(error);
        goto abort;
      }
    }

    if ((error = m_hyperspace_ptr->attr_set(handle, "table_id", &table_id, sizeof(int32_t))) != Error::OK) {
      errMsg = (std::string)"Problem setting attribute 'table_id' of file " + tableFile + " - " + Error::get_text(error);
      goto abort;
    }
  }


  /**
   * Write schema attribute
   */
  if ((error = m_hyperspace_ptr->attr_set(handle, "schema", finalSchema.c_str(), strlen(finalSchema.c_str()))) != Error::OK) {
    errMsg = (std::string)"Problem creating attribute 'schema' for table file '" + tableFile + "'";
    goto abort;
  }

  m_hyperspace_ptr->close(handle);

  /**
   * Create /hypertable/tables/&lt;table&gt;/&lt;accessGroup&gt; directories for this table in HDFS
   */
  tableBaseDir = (string)"/hypertable/tables/" + tableName + "/";
  lgList = schema->get_access_group_list();
  for (list<Schema::AccessGroup *>::iterator lgIter = lgList->begin(); lgIter != lgList->end(); lgIter++) {
    lgDir = tableBaseDir + (*lgIter)->name;
    if ((error = m_dfs_client->mkdirs(lgDir)) != Error::OK) {
      errMsg = (string)"Problem creating table directory '" + lgDir + "'";
      goto abort;
    }
  }

  /**
   * Write METADATA entry, single range covering entire table '\\0' to 0xff 0xff
   */
  if (table_id != 0) {
    TableMutatorPtr mutator_ptr;
    KeySpec key;
    std::string metadata_key_str;
    struct sockaddr_in addr;

    if ((error = m_metadata_table_ptr->create_mutator(mutator_ptr)) != Error::OK) {
      // TODO: throw exception
      HT_ERROR("Problem creating mutator on METADATA table");
      exit(1);
    }

    metadata_key_str = std::string("") + table_id + ":" + Key::END_ROW_MARKER;
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
      // TODO: propagate exception
      HT_ERRORF("Problem updating METADATA with new table info (row key = %s) - %s", metadata_key_str.c_str(), e.what());
      exit(1);
    }

    /**
     * TEMPORARY:  ask the one Range Server that we know about to load the range
     */

    TableIdentifierT table;
    RangeT range;
    uint64_t soft_limit;
    RangeServerClient rsc(m_conn_manager_ptr->get_comm(), 30);

    table.name = tableName;
    table.id = table_id;
    table.generation = schema->get_generation();

    range.startRow = 0;
    range.endRow = Key::END_ROW_MARKER;

    {
      boost::mutex::scoped_lock lock(m_mutex);
      if (m_server_map_iter == m_server_map.end())
        m_server_map_iter = m_server_map.begin();
      assert(m_server_map_iter != m_server_map.end());
      memcpy(&addr, &((*m_server_map_iter).second->addr), sizeof(struct sockaddr_in));
      HT_INFOF("Assigning first range %s[%s:%s] to %s", table.name, range.startRow, range.endRow, (*m_server_map_iter).first.c_str());
      m_server_map_iter++;
      soft_limit = m_max_range_bytes / std::min(64, (int)m_server_map.size()*2);
    }

    if ((error = rsc.load_range(addr, table, range, soft_limit, 0)) != Error::OK) {
      std::string addrStr;
      HT_ERRORF("Problem issuing 'load range' command for %s[..%s] at server %s",
		table.name, range.endRow, InetAddr::string_format(addrStr, addr));
    }
  }

  if (m_verbose) {
    HT_INFOF("Successfully created table '%s' ID=%d", tableName, table_id);
  }

abort:
  m_hyperspace_ptr->set_silent_flag(false);
  delete schema;
  if (error != Error::OK) {
    if (m_verbose) {
      HT_ERRORF("%s '%s'", Error::get_text(error), errMsg.c_str());
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
  HandleCallbackPtr nullHandleCallback;

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
  if ((error = m_hyperspace_ptr->open("/hypertable/master", OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE, nullHandleCallback, &handle)) != Error::OK) {
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
  if ((error = m_hyperspace_ptr->open("/hypertable/root", OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE, nullHandleCallback, &handle)) != Error::OK) {
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
  HandleCallbackPtr lockFileHandler;
  std::vector<struct DirEntryT> listing;
  uint32_t lockStatus;
  struct LockSequencerT lockSequencer;
  RangeServerStatePtr statePtr;
  uint32_t oflags;
  std::string hsFilename;

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

    statePtr = new RangeServerState();
    statePtr->location = listing[i].name;

    hsFilename = (std::string)"/hypertable/servers/" + listing[i].name;

    lockFileHandler = new ServerLockFileHandler(statePtr, this, m_app_queue_ptr);

    if ((error = m_hyperspace_ptr->open(hsFilename, oflags, lockFileHandler, &statePtr->hyperspaceHandle)) != Error::OK) {
      HT_ERRORF("Problem opening discovered server file %s - %s", hsFilename.c_str(), Error::get_text(error));
      continue;
    }

    if ((error = m_hyperspace_ptr->try_lock(statePtr->hyperspaceHandle, LOCK_MODE_EXCLUSIVE, &lockStatus, &lockSequencer)) != Error::OK) {
      HT_ERRORF("Problem obtaining exclusive lock on master Hyperspace file '/hypertable/master' - %s", Error::get_text(error));
      continue;
    }

    if (lockStatus == LOCK_STATUS_GRANTED) {
      HT_INFOF("Obtained lock on servers file %s, removing...", hsFilename.c_str());
      if ((error = m_hyperspace_ptr->unlink(hsFilename)) != Error::OK)
        HT_INFOF("Problem deleting Hyperspace file %s", hsFilename.c_str());
      if ((error = m_hyperspace_ptr->close(statePtr->hyperspaceHandle)) != Error::OK)
        HT_INFOF("Problem closing handle on deleting Hyperspace file %s", hsFilename.c_str());
    }
    else {
      m_server_map[statePtr->location] = statePtr;
    }
  }

}


/**
 *
 */
bool Master::create_hyperspace_dir(std::string dir) {
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
