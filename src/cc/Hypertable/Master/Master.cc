/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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

#include <string>

extern "C" {
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/types.h>
}

#include "Common/FileUtils.h"
#include "Common/InetAddr.h"
#include "Common/System.h"

#include "DfsBroker/Lib/Client.h"
#include "Hypertable/Lib/RangeServerClient.h"
#include "Hypertable/Lib/Schema.h"
#include "Hyperspace/DirEntry.h"

#include "Master.h"
#include "ServersDirectoryHandler.h"
#include "ServerLockFileHandler.h"
#include "RangeServerState.h"

using namespace Hyperspace;
using namespace Hypertable;
using namespace Hypertable::DfsBroker;
using namespace std;

Master::Master(ConnectionManagerPtr &connManagerPtr, PropertiesPtr &propsPtr, ApplicationQueuePtr &appQueuePtr) : m_conn_manager_ptr(connManagerPtr), m_app_queue_ptr(appQueuePtr), m_verbose(false), m_dfs_client(0) {
  int error;
  Client *dfsClient;
  uint16_t port;

  m_hyperspace_ptr = new Hyperspace::Session(connManagerPtr->get_comm(), propsPtr, &m_hyperspace_session_handler);

  if (!m_hyperspace_ptr->wait_for_connection(30)) {
    LOG_ERROR("Unable to connect to hyperspace, exiting...");
    exit(1);
  }

  m_verbose = propsPtr->getPropertyBool("verbose", false);

  if ((port = (uint16_t)propsPtr->getPropertyInt("Hypertable.Master.port", 0)) == 0) {
    LOG_ERROR("Hypertable.Master.port property not found in config file, exiting...");
    exit(1);
  }

  /**
   * Create DFS Client connection
   */
  dfsClient = new Client(connManagerPtr, propsPtr);

  if (m_verbose) {
    cout << "DfsBroker.host=" << propsPtr->getProperty("DfsBroker.host", "") << endl;
    cout << "DfsBroker.port=" << propsPtr->getProperty("DfsBroker.port", "") << endl;
    cout << "DfsBroker.timeout=" << propsPtr->getProperty("DfsBroker.timeout", "") << endl;
  }

  if (!dfsClient->wait_for_connection(30)) {
    LOG_ERROR("Unable to connect to DFS Broker, exiting...");
    exit(1);
  }

  m_dfs_client = dfsClient;

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
      LOG_VA_ERROR("Unable to open Hyperspace file '/hypertable/master' (%s)  Try re-running with --initialize",
		   Error::get_text(error));
      exit(1);
    }

    if ((error = m_hyperspace_ptr->try_lock(m_master_file_handle, LOCK_MODE_EXCLUSIVE, &lockStatus, &m_master_file_sequencer)) != Error::OK) {
      LOG_VA_ERROR("Problem obtaining exclusive lock on master Hyperspace file '/hypertable/master' - %s", Error::get_text(error));
      exit(1);
    }
    
    if (lockStatus != LOCK_STATUS_GRANTED) {
      LOG_ERROR("Unable to obtain lock on '/hypertable/master' - conflict");
      exit(1);
    }

    // Write master location in 'address' attribute, format is IP:port
    {
      std::string hostStr, addrStr;
      struct hostent *he;
      InetAddr::get_hostname(hostStr);
      he = gethostbyname(hostStr.c_str());
      addrStr = std::string(inet_ntoa(*(struct in_addr *)*he->h_addr_list)) + ":" + (int)port;
      if ((error = m_hyperspace_ptr->attr_set(m_master_file_handle, "address", addrStr.c_str(), strlen(addrStr.c_str()))) != Error::OK) {
	LOG_VA_ERROR("Problem setting attribute 'address' of hyperspace file /hypertable/master - %s", Error::get_text(error));
	exit(1);
      }
    }

    if ((error = m_hyperspace_ptr->attr_get(m_master_file_handle, "last_table_id", valueBuf)) != Error::OK) {
      LOG_VA_ERROR("Problem getting attribute 'last_table_id' from file /hypertable/master - %s", Error::get_text(error));
      exit(1);
    }

    assert(valueBuf.fill() == sizeof(int32_t));

    memcpy(&ival, valueBuf.buf, sizeof(int32_t));

    atomic_set(&m_last_table_id, ival);
    if (m_verbose)
      cout << "Last Table ID: " << ival << endl;
  }

  /**
   * Locate tablet servers
   */
  scan_servers_directory();

}



Master::~Master() {
  delete m_dfs_client;
}



/**
 *
 */
void Master::server_joined(std::string &location) {
  int error;
  RangeServerStatePtr statePtr;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
  HandleCallbackPtr lockFileHandler;
  uint32_t lockStatus;
  struct LockSequencerT lockSequencer;
  ServerMapT::iterator iter;
  std::string hsFilename;
  
  {
    boost::mutex::scoped_lock lock(m_mutex);
    if ((iter = m_server_map.find(location)) != m_server_map.end())
      statePtr = (*iter).second;
    else {
      statePtr = new RangeServerState();
      statePtr->location = location;
    }
  }

  LOG_VA_INFO("Server Joined (%s)", location.c_str());

  hsFilename = (std::string)"/hypertable/servers/" + location;

  lockFileHandler = new ServerLockFileHandler(statePtr, this, m_app_queue_ptr);

  if ((error = m_hyperspace_ptr->open(hsFilename, oflags, lockFileHandler, &statePtr->hyperspaceHandle)) != Error::OK) {
    LOG_VA_ERROR("Problem opening discovered server file %s - %s", hsFilename.c_str(), Error::get_text(error));
    return;
  }

  if ((error = m_hyperspace_ptr->try_lock(statePtr->hyperspaceHandle, LOCK_MODE_EXCLUSIVE, &lockStatus, &lockSequencer)) != Error::OK) {
    LOG_VA_ERROR("Problem attempting to obtain exclusive lock on server Hyperspace file '%s' - %s",
		 hsFilename.c_str(), Error::get_text(error));
    return;
  }

  if (lockStatus == LOCK_STATUS_GRANTED) {
    LOG_VA_INFO("Obtained lock on servers file %s, removing...", hsFilename.c_str());
    if ((error = m_hyperspace_ptr->unlink(hsFilename)) != Error::OK)
      LOG_VA_INFO("Problem deleting Hyperspace file %s", hsFilename.c_str());
    if ((error = m_hyperspace_ptr->close(statePtr->hyperspaceHandle)) != Error::OK)
      LOG_VA_INFO("Problem closing handle on deleting Hyperspace file %s", hsFilename.c_str());
  }
  else {
    boost::mutex::scoped_lock lock(m_mutex);
    m_server_map[statePtr->location] = statePtr;  
  }
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
    LOG_VA_WARN("Server (%s) not found in map", location.c_str());
    return;
  }

  if ((error = m_hyperspace_ptr->try_lock((*iter).second->hyperspaceHandle, LOCK_MODE_EXCLUSIVE, &lockStatus, &lockSequencer)) != Error::OK) {
    LOG_VA_ERROR("Problem obtaining exclusive lock on master Hyperspace file '/hypertable/master' - %s", Error::get_text(error));
    return;
  }

  if (lockStatus != LOCK_STATUS_GRANTED) {
    LOG_VA_INFO("Unable to obtain lock on server file %s, ignoring...", location.c_str());
    return;
  }

  m_hyperspace_ptr->unlink(hsFilename);
  m_hyperspace_ptr->close((*iter).second->hyperspaceHandle);
  m_server_map.erase(iter);

  LOG_VA_INFO("RangeServer lost it's lock on file %s, deleting ...", hsFilename.c_str());
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
  if ((error = create_table(tableName, schemaString, errMsg)) != Error::OK)
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
    LOG_VA_INFO("Successfully fetched schema (length=%d) for table '%s'", strlen((char *)schemaBuf.buf), tableName);
  }

 abort:
  if (error != Error::OK) {
    if (m_verbose) {
      LOG_VA_ERROR("%s '%s'", Error::get_text(error), errMsg.c_str());
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
  RangeServerStatePtr statePtr;
  std::string idStr = location;
  ServerMapT::iterator iter = m_server_map.find(idStr);

  if ((iter = m_server_map.find(location)) != m_server_map.end())
    statePtr = (*iter).second;
  else {
    statePtr = new RangeServerState();
    statePtr->location = location;
    m_server_map[statePtr->location] = statePtr;
  }

  statePtr->addr = addr;

  {
    std::string addrStr;
    LOG_VA_INFO("Server Registered %s -> %s", location, InetAddr::string_format(addrStr, addr));
    cout << flush;
  }

  cb->response_ok();
}

/**
 * Just turns around and assigns new range to caller
 */
void Master::report_split(ResponseCallback *cb, TableIdentifierT &table, RangeT &range) {
  boost::mutex::scoped_lock lock(m_mutex);
  int error;
  struct sockaddr_in addr;
  RangeServerClient rsc(m_conn_manager_ptr->get_comm(), 30);

  cb->response_ok();  

  cb->get_address(addr);

  if ((error = rsc.load_range(addr, table, range, 0)) != Error::OK) {
    std::string addrStr;
    LOG_VA_ERROR("Problem issuing 'load range' command for %s[%s:%s] at server %s",
		 table.name, range.startRow, range.endRow, InetAddr::string_format(addrStr, addr));
  }
  else
    LOG_VA_INFO("report_split for %s[%s:%s] successful.", table.name, range.startRow, range.endRow);

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
  uint32_t tableId;

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
    tableId = (uint32_t)atomic_inc_return(&m_last_table_id);

    if ((error = m_hyperspace_ptr->attr_set(m_master_file_handle, "last_table_id", &tableId, sizeof(int32_t))) != Error::OK) {
      errMsg = (std::string)"Problem setting attribute 'last_table_id' of file /hypertable/master - " + Error::get_text(error);
      goto abort;
    }

    if ((error = m_hyperspace_ptr->attr_set(handle, "table_id", &tableId, sizeof(int32_t))) != Error::OK) {
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
   * Create /hypertable/tables/<table>/<accessGroup> directories for this table in HDFS
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

  if (m_verbose) {
    LOG_VA_INFO("Successfully created table '%s' ID=%d", tableName, tableId);
  }

 abort:
  delete schema;
  if (error != Error::OK) {
    if (m_verbose) {
      LOG_VA_ERROR("%s '%s'", Error::get_text(error), errMsg.c_str());
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
  uint32_t tableId = 0;
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
    LOG_VA_ERROR("Unable to open Hyperspace file '/hypertable/master' (%s)", Error::get_text(error));
    return false;
  }
  m_master_file_handle = handle;

  atomic_set(&m_last_table_id, -1);

  /**
   * Create METADATA table
   */
  {
    std::string errMsg;
    std::string metadataSchemaFile = System::installDir + "/conf/METADATA.xml";
    off_t schemaLen;
    const char *schemaStr = FileUtils::file_to_buffer(metadataSchemaFile.c_str(), &schemaLen);

    if ((error = create_table("METADATA", schemaStr, errMsg)) != Error::OK) {
      LOG_VA_ERROR("Problem creating METADATA table - %s", Error::get_text(error));
      return false;
    }
  }

  m_hyperspace_ptr->close(handle);
  m_master_file_handle = 0;

  /**
   *  Create /hypertable/root
   */
  if ((error = m_hyperspace_ptr->open("/hypertable/root", OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE, nullHandleCallback, &handle)) != Error::OK) {
    LOG_VA_ERROR("Unable to open Hyperspace file '/hypertable/root' (%s)", Error::get_text(error));
    return false;
  }
  m_hyperspace_ptr->close(handle);

  LOG_INFO("Successfully Initialized Hypertable.");

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
    LOG_VA_ERROR("Unable to open Hyperspace directory '/hypertable/servers' (%s)  Try re-running with --initialize",
		 Error::get_text(error));
    exit(1);
  }

  if ((error = m_hyperspace_ptr->readdir(m_servers_dir_handle, listing)) != Error::OK) {
    LOG_VA_ERROR("Problem scanning Hyperspace directory '/hypertable/servers' - %s", Error::get_text(error));
    exit(1);
  }

  oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;

  for (size_t i=0; i<listing.size(); i++) {

    statePtr = new RangeServerState();
    statePtr->location = listing[i].name;

    hsFilename = (std::string)"/hypertable/servers/" + listing[i].name;

    lockFileHandler = new ServerLockFileHandler(statePtr, this, m_app_queue_ptr);

    if ((error = m_hyperspace_ptr->open(hsFilename, oflags, lockFileHandler, &statePtr->hyperspaceHandle)) != Error::OK) {
      LOG_VA_ERROR("Problem opening discovered server file %s - %s", hsFilename.c_str(), Error::get_text(error));
      continue;
    }

    if ((error = m_hyperspace_ptr->try_lock(statePtr->hyperspaceHandle, LOCK_MODE_EXCLUSIVE, &lockStatus, &lockSequencer)) != Error::OK) {
      LOG_VA_ERROR("Problem obtaining exclusive lock on master Hyperspace file '/hypertable/master' - %s", Error::get_text(error));
      continue;
    }

    if (lockStatus == LOCK_STATUS_GRANTED) {
      LOG_VA_INFO("Obtained lock on servers file %s, removing...", hsFilename.c_str());
      if ((error = m_hyperspace_ptr->unlink(hsFilename)) != Error::OK)
	LOG_VA_INFO("Problem deleting Hyperspace file %s", hsFilename.c_str());
      if ((error = m_hyperspace_ptr->close(statePtr->hyperspaceHandle)) != Error::OK)
	LOG_VA_INFO("Problem closing handle on deleting Hyperspace file %s", hsFilename.c_str());
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
    LOG_VA_ERROR("Problem checking for existence of directory '%s' - %s", dir.c_str(), Error::get_text(error));
    return false;
  }

  if (exists)
    return true;

  if ((error = m_hyperspace_ptr->mkdir(dir)) != Error::OK) {
    LOG_VA_ERROR("Problem creating directory '%s' - %s", dir.c_str(), Error::get_text(error));
    return false;
  }

  return true;
}


