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

#include <cassert>

#include "AsyncComm/Comm.h"

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"

#include "Client.h"

using namespace Hyperspace;

/**
 *
 */
Client::Client(std::string configFile) {
  uint16_t masterPort;
  const char *masterHost;
  struct sockaddr_in addr;
  PropertiesPtr propsPtr( new Properties(configFile) );

  mComm = new Comm();
  mConnManagerPtr = new ConnectionManager(mComm);

  mHyperspacePtr = new Hyperspace::Session(mComm, propsPtr);
  if (!mHyperspacePtr->WaitForConnection(30)) {
    LOG_ERROR("Unable to connect to hyperspace, exiting...");
    exit(1);
  }

  mAppQueuePtr = new ApplicationQueue(1);

  mMasterClientPtr = new MasterClient(mConnManagerPtr, mHyperspacePtr, 20, mAppQueuePtr);
  if (mMasterClientPtr->InitiateConnection(0) != Error::OK) {
    LOG_ERROR("Unable to establish connection with Master, exiting...");
    exit(1);
  }
}


/**
 * 
 */
int Client::CreateTable(std::string name, std::string schema) {
  return mMasterClientPtr->CreateTable(name.c_str(), schema.c_str());
}


/**
 *
 */
int Client::OpenTable(std::string name, TablePtr &tablePtr) {
  Table *table;
  try {
    table = new Table(mConnManagerPtr, mHyperspacePtr, name);
  }
  catch (Exception &e) {
    LOG_VA_ERROR("Problem opening table '%s' - %s", name.c_str(), e.what());
    return e.code();
  }
  tablePtr = table;
  return Error::OK;
}



/**
 * 
 */
int Client::GetSchema(std::string tableName, std::string &schema) {
  return mMasterClientPtr->GetSchema(tableName.c_str(), schema);
}

