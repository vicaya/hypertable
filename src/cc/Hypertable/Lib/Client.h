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

#ifndef HYPERTABLE_CLIENT_H
#define HYPERTABLE_CLIENT_H

#include <string>

#include "Common/Properties.h"
#include "Common/ReferenceCount.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/ConnectionManager.h"
#include "Hyperspace/Session.h"

#include "MasterClient.h"
#include "Table.h"


namespace Hypertable {

  class Comm;
  class HqlCommandInterpreter;

  class Client : public ReferenceCount {

  public:

    Client(const char *argv0, std::string configFile);
    Client(const char *argv0);
    void initialize(std::string config_file);
    int create_table(std::string name, std::string schema);
    int open_table(std::string name, TablePtr &tablePtr);
    int get_table_id(std::string name, uint32_t *table_idp);
    int get_schema(std::string tableName, std::string &schema);
    int get_tables(std::vector<std::string> &tables);
    int drop_table(std::string name, bool if_exists);

    HqlCommandInterpreter *create_hql_interpreter();

  private:
    PropertiesPtr           m_props_ptr;
    Comm                   *m_comm;
    ConnectionManagerPtr    m_conn_manager_ptr;
    ApplicationQueuePtr     m_app_queue_ptr;
    Hyperspace::SessionPtr  m_hyperspace_ptr;
    MasterClientPtr         m_master_client_ptr;
  };
  typedef boost::intrusive_ptr<Client> ClientPtr;
  

}

#endif // HYPERTABLE_CLIENT_H

