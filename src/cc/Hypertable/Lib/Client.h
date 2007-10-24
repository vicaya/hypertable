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

#ifndef HYPERTABLE_CLIENT_H
#define HYPERTABLE_CLIENT_H

#include <string>

#include "Table.h"

namespace Hyperspace {
  class Session;
}

namespace hypertable {

  class ApplicationQueue;
  class Comm;
  class ConnectionManager;
  class MasterClient;

  class Client {

  public:

    Client(std::string configFile);

    int CreateTable(std::string name, std::string schema);
    int OpenTable(std::string name, TablePtr &tablePtr);
    int GetSchema(std::string tableName, std::string &schema);

    //Table OpenTable();
    //void DeleteTable();
    // String [] ListTables();

  private:
    ApplicationQueue     *mAppQueue;
    Comm                 *mComm;
    ConnectionManager    *mConnManager;
    Hyperspace::Session  *mHyperspace;
    MasterClient         *mMasterClient;
  };

}

#endif // HYPERTABLE_CLIENT_H

