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

#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"

#include "Manager.h"


Manager *Manager::msInstance = 0;


/**
 *
 */
Manager::Manager(PropertiesPtr &propsPtr) {

  mComm = new Comm();
  mConnectionManager = new ConnectionManager(mComm);

  /**
   *  Create Master
   */
  mMasterPtr.reset( new MasterClient(mConnectionManager, propsPtr) );

  if (!mMasterPtr->WaitForConnection(60)) {
    cerr << "Unable to connect to Master, exiting..." << endl;
    exit(1);
  }
  
}


/**
 *
 */
void Manager::Initialize(std::string configFile) {
  PropertiesPtr propsPtr( new Properties(configFile) );

  ReactorFactory::Initialize((uint16_t)System::GetProcessorCount());

  if (msInstance != 0) {
    LOG_ERROR("Initialize can only be called once.");
    DUMP_CORE;
  }

  msInstance = new Manager(propsPtr);

}

/**
 * 
 */
int Manager::CreateTable(std::string name, std::string schema) {
  return mMasterPtr->CreateTable(name.c_str(), schema.c_str());
}


/**
 * 
 */
int Manager::GetSchema(std::string tableName, std::string &schema) {
  return mMasterPtr->GetSchema(tableName.c_str(), schema);
}

