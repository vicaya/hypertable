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

#include "Hypertable/Lib/Schema.h"

#include "Master.h"

using namespace hypertable;
using namespace std;

Master::Master(ConnectionManager *connManager, Properties *props) : mVerbose(false), mHyperspace(0), mHdfsClient(0) {

  mHyperspace = new HyperspaceClient(connManager, props);

  if (!mHyperspace->WaitForConnection(30)) {
    LOG_ERROR("Unable to connect to hyperspace, exiting...");
    exit(1);
  }

  mVerbose = props->getPropertyBool("verbose", false);

  /**
   * Create HDFS Client connection
   */
  {
    struct sockaddr_in addr;
    const char *host;
    int port;

    if ((host = props->getProperty("HdfsBroker.host", 0)) == 0) {
      LOG_ERROR("HdfsBroker.host property not specified");
      exit(1);
    }

    if ((port = props->getPropertyInt("HdfsBroker.port", 0)) == 0) {
      LOG_ERROR("HdfsBroker.port property not specified");
      exit(1);
    }

    if (mVerbose) {
      cout << "HdfsBroker.host=" << host << endl;
      cout << "HdfsBroker.port=" << port << endl;
    }

    memset(&addr, 0, sizeof(struct sockaddr_in));
    {
      struct hostent *he = gethostbyname(host);
      if (he == 0) {
	herror("gethostbyname()");
	exit(1);
      }
      memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    mHdfsClient = new HdfsClient(connManager, addr, 20);
    if (!mHdfsClient->WaitForConnection(30)) {
      LOG_ERROR("Unable to connect to HdfsBroker, exiting...");
      exit(1);
    }
  }

  /* Read Last Table ID */
  {
    DynamicBuffer valueBuf(0);
    int error;

    if ((error = mHyperspace->AttrGet("/hypertable/meta", "last_table_id", valueBuf)) != Error::OK) {
      LOG_ERROR("/hypertable/meta file not found in hyperspace.  Try re-running with --initialize.");
      exit(1);
    }

    if (valueBuf.fill() == 0) {
      LOG_ERROR("Empty value for attribute 'last_table_id' of file '/hypertable/meta' in hyperspace");
      exit(1);
    }
    else {
      int ival = strtol((const char *)valueBuf.buf, 0, 0);
      if (ival == 0 && errno == EINVAL) {
	LOG_VA_ERROR("Problem converting attribute 'last_table_id' (%s) to an integer", (const char *)valueBuf.buf);
	exit(1);
      }
      atomic_set(&mLastTableId, ival);
      if (mVerbose)
	cout << "Last Table ID: " << ival << endl;
    }
  }

}

Master::~Master() {
  delete mHyperspace;
  delete mHdfsClient;
}

void Master::CreateTable(ResponseCallback *cb, const char *tableName, const char *schemaString) {
  int error = Error::OK;
  std::string finalSchema = "";
  std::string tableFile = (std::string)"/hypertable/tables/" + tableName;
  std::string errMsg;
  string tableBaseDir;
  string lgDir;
  Schema *schema = 0;
  list<Schema::AccessGroup *> *lgList;

  /**
   * Check for table existence
   */
  if ((error = mHyperspace->Exists(tableFile.c_str())) == Error::OK) {
    errMsg = tableName;
    error = Error::MASTER_TABLE_EXISTS;
    goto abort;
  }
  else if (error != Error::HYPERSPACE_FILE_NOT_FOUND) {
    errMsg = (std::string)"Problem checking for existence of table file '" + tableFile + "'";
    goto abort;
  }

  /**
   *  Parse Schema and assign Generation number and Column ids
   */
  schema = Schema::NewInstance(schemaString, strlen(schemaString));
  if (!schema->IsValid()) {
    errMsg = schema->GetErrorString();
    error = Error::MASTER_BAD_SCHEMA;
    return;
  }
  schema->AssignIds();
  schema->Render(finalSchema);

  /**
   * Create table file
   */
  if ((error = mHyperspace->Create(tableFile.c_str())) != Error::OK) {
    errMsg = "Problem creating table file '" + tableFile + "'";
    goto abort;
  }


  /**
   * Write 'table_id' attribute of table file and 'last_table_id' attribute of /hypertable/meta
   */
  {
    char buf[32];
    int tableId = atomic_inc_return(&mLastTableId);
    sprintf(buf, "%d", tableId);
    if ((error = mHyperspace->AttrSet("/hypertable/meta", "last_table_id", buf)) != Error::OK) {
      errMsg = "Problem updating attribute 'last_table_id' of file '/hypertable/meta'";
      goto abort;
    }
    if ((error = mHyperspace->AttrSet(tableFile.c_str(), "table_id", buf)) != Error::OK) {
      errMsg = "Problem creating attribute 'table_id' for table file '" + tableFile + "'";
      goto abort;
    }
  }


  /**
   * Write schema attribute
   */
  if ((error = mHyperspace->AttrSet(tableFile.c_str(), "schema", finalSchema.c_str())) != Error::OK) {
    errMsg = "Problem creating attribute 'schema' for table file '" + tableFile + "'";
    goto abort;
  }

  /**
   * Create /hypertable/tables/<table>/<accessGroup> directories for this table in HDFS
   */
  tableBaseDir = (string)"/hypertable/tables/" + tableName + "/";
  lgList = schema->GetAccessGroupList();
  for (list<Schema::AccessGroup *>::iterator lgIter = lgList->begin(); lgIter != lgList->end(); lgIter++) {
    lgDir = tableBaseDir + (*lgIter)->name;
    if ((error = mHdfsClient->Mkdirs(lgDir.c_str())) != Error::OK) {
      errMsg = (string)"Problem creating table directory '" + lgDir + "'";
      goto abort;
    }
  }

  cb->response_ok();

  if (mVerbose) {
    LOG_VA_INFO("Successfully created table '%s'", tableName);
  }

 abort:
  delete schema;
  if (error != Error::OK) {
    if (mVerbose) {
      LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    }
    cb->error(error, errMsg);
  }
}

void Master::GetSchema(ResponseCallbackGetSchema *cb, const char *tableName) {
  int error = Error::OK;
  std::string tableFile = (std::string)"/hypertable/tables/" + tableName;
  std::string errMsg;
  DynamicBuffer schemaBuf(0);

  /**
   * Check for table existence
   */
  if ((error = mHyperspace->Exists(tableFile.c_str())) != Error::OK) {
    errMsg = tableName;
    goto abort;
  }

  /**
   * Get schema attribute
   */
  if ((error = mHyperspace->AttrGet(tableFile.c_str(), "schema", schemaBuf)) != Error::OK) {
    errMsg = "Problem getting attribute 'schema' for table file '" + tableFile + "'";
    goto abort;
  }
  
  cb->response((char *)schemaBuf.buf);

  if (mVerbose) {
    LOG_VA_INFO("Successfully fetched schema (length=%d) for table '%s'", strlen((char *)schemaBuf.buf), tableName);
  }

 abort:
  if (error != Error::OK) {
    if (mVerbose) {
      LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    }
    cb->error(error, errMsg);
  }
  return;
}



bool Master::CreateDirectoryLayout() {
  int error;

  /**
   * Create /hypertable/servers directory
   */
  error = mHyperspace->Exists("/hypertable/servers");
  if (error == Error::HYPERSPACE_FILE_NOT_FOUND) {
    if ((error = mHyperspace->Mkdirs("/hypertable/servers")) != Error::OK) {
      LOG_VA_ERROR("Problem creating hyperspace directory '/hypertable/servers' - %s", Error::GetText(error));
      return false;
    }
  }
  else if (error != Error::OK)
    return false;


  /**
   * Create /hypertable/tables directory
   */
  error = mHyperspace->Exists("/hypertable/tables");
  if (error == Error::HYPERSPACE_FILE_NOT_FOUND) {
    if (mHyperspace->Mkdirs("/hypertable/tables") != Error::OK)
      return false;
  }
  else if (error != Error::OK)
    return false;


  /**
   * Create /hypertable/master directory
   */
  error = mHyperspace->Exists("/hypertable/master");
  if (error == Error::HYPERSPACE_FILE_NOT_FOUND) {
    if (mHyperspace->Mkdirs("/hypertable/master") != Error::OK)
      return false;
  }
  else if (error != Error::OK)
    return false;

  /**
   * Create /hypertable/meta directory
   */
  error = mHyperspace->Exists("/hypertable/meta");
  if (error == Error::HYPERSPACE_FILE_NOT_FOUND) {
    if (mHyperspace->Mkdirs("/hypertable/meta") != Error::OK)
      return false;
  }
  else if (error != Error::OK)
    return false;

  /**
   * 
   */
  if ((error = mHyperspace->AttrSet("/hypertable/meta", "last_table_id", "0")) != Error::OK)
    return false;

  return true;
}
