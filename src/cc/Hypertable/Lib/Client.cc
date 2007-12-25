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

extern "C" {
#include <poll.h>
}

#include "AsyncComm/Comm.h"

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/System.h"

#include "Client.h"
#include "HqlCommandInterpreter.h"

using namespace Hyperspace;

/**
 *
 */
Client::Client(std::string configFile) {
  time_t master_timeout;

  m_props_ptr = new Properties(configFile);

  m_comm = new Comm();
  m_conn_manager_ptr = new ConnectionManager(m_comm);

  m_hyperspace_ptr = new Hyperspace::Session(m_comm, m_props_ptr);
  while (!m_hyperspace_ptr->wait_for_connection(2)) {
    cout << "Waiting for connection to Hyperspace..." << flush;
    poll(0, 0, 5000);
    cout << endl << flush;
  }

  m_app_queue_ptr = new ApplicationQueue(1);

  master_timeout = m_props_ptr->getPropertyInt("Hypertable.Master.Client.Timeout", 180);

  m_master_client_ptr = new MasterClient(m_conn_manager_ptr, m_hyperspace_ptr, master_timeout, m_app_queue_ptr);
  if (m_master_client_ptr->initiate_connection(0) != Error::OK) {
    LOG_ERROR("Unable to establish connection with Master, exiting...");
    exit(1);
  }
}


/**
 * 
 */
int Client::create_table(std::string name, std::string schema) {
  return m_master_client_ptr->create_table(name.c_str(), schema.c_str());
}


/**
 *
 */
int Client::open_table(std::string name, TablePtr &tablePtr) {
  Table *table;
  try {
    table = new Table(m_props_ptr, m_conn_manager_ptr, m_hyperspace_ptr, name);
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
int Client::get_schema(std::string tableName, std::string &schema) {
  return m_master_client_ptr->get_schema(tableName.c_str(), schema);
}


HqlCommandInterpreter *Client::create_hql_interpreter() {
  return new HqlCommandInterpreter(this);
}
