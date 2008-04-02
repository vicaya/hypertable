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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <cassert>

extern "C" {
#include <poll.h>
}

#include "AsyncComm/Comm.h"
#include "AsyncComm/ReactorFactory.h"

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/System.h"

#include "Hyperspace/DirEntry.h"

#include "Client.h"
#include "HqlCommandInterpreter.h"

using namespace Hyperspace;

/**
 *
 */
Client::Client(const char *argv0, const String &config_file) {
  System::initialize(argv0);
  ReactorFactory::initialize((uint16_t)System::get_processor_count());
  initialize(config_file);
}

Client::Client(const char *argv0) {
  System::initialize(argv0);
  ReactorFactory::initialize((uint16_t)System::get_processor_count());
  initialize( System::installDir + "/conf/hypertable.cfg" );
}

/**
 * 
 */
void Client::create_table(const String &name, const String &schema) {
  int error;
  if ((error = m_master_client_ptr->create_table(name.c_str(), schema.c_str())) != Error::OK)
    throw Exception(error);
}


/**
 *
 */
Table *Client::open_table(const String &name) {
  return new Table(m_props_ptr, m_conn_manager_ptr, m_hyperspace_ptr, name);
}


/**
 * 
 */
uint32_t Client::get_table_id(const String &name) {
  int error;
  String table_file = (String)"/hypertable/tables/" + name;
  DynamicBuffer value_buf(0);
  HandleCallbackPtr nullHandleCallback;
  uint64_t handle;
  uint32_t uval;

  /**
   * Open table file in Hyperspace
   */
  if ((error = m_hyperspace_ptr->open(table_file.c_str(), OPEN_FLAG_READ, nullHandleCallback, &handle)) != Error::OK)
    throw Exception(error, (String)"Hyperspace::open(" + table_file + ") failed");

  /**
   * Get the 'table_id' attribute
   */
  if ((error = m_hyperspace_ptr->attr_get(handle, "table_id", value_buf)) != Error::OK)
    throw Exception(error, (String)"Hyperspace::attr_get(file='" + table_file + "', attr=table_id) failed");

  /**
   * Close the hyperspace file
   */
  if ((error = m_hyperspace_ptr->close(handle)) != Error::OK)
    throw Exception(error);

  assert(value_buf.fill() == sizeof(int32_t));

  memcpy(&uval, value_buf.buf, sizeof(int32_t));

  return uval;
}


/**
 * 
 */
String Client::get_schema(const String &name) {
  int error;
  String schema;
  if ((error = m_master_client_ptr->get_schema(name.c_str(), schema)) != Error::OK)
    throw Exception(error);
  return schema;
}



/**
 */
void Client::get_tables(std::vector<String> &tables) {
  int error;
  uint64_t handle;
  HandleCallbackPtr nullHandleCallback;
  std::vector<struct Hyperspace::DirEntryT> listing;

  if ((error = m_hyperspace_ptr->open("/hypertable/tables", OPEN_FLAG_READ, nullHandleCallback, &handle)) != Error::OK)
    throw Exception(error);

  if ((error = m_hyperspace_ptr->readdir(handle, listing)) != Error::OK)
    throw Exception(error);

  for (size_t i=0; i<listing.size(); i++) {
    if (!listing[i].isDirectory)
      tables.push_back(listing[i].name);
  }

  m_hyperspace_ptr->close(handle);

}


void Client::drop_table(const String &name, bool if_exists) {
  int error;
  if ((error = m_master_client_ptr->drop_table(name.c_str(), if_exists)) != Error::OK)
    throw Exception(error);
}


HqlCommandInterpreter *Client::create_hql_interpreter() {
  return new HqlCommandInterpreter(this);
}



// ------------- PRIVATE METHODS -----------------



void Client::initialize(const String &config_file) {
  time_t master_timeout;

  m_props_ptr = new Properties(config_file);

  m_comm = new Comm();
  m_conn_manager_ptr = new ConnectionManager(m_comm);

  m_hyperspace_ptr = new Hyperspace::Session(m_comm, m_props_ptr);
  while (!m_hyperspace_ptr->wait_for_connection(2)) {
    cout << "Waiting for connection to Hyperspace..." << flush;
    poll(0, 0, 5000);
    cout << endl << flush;
  }

  m_app_queue_ptr = new ApplicationQueue(1);

  master_timeout = m_props_ptr->get_int("Hypertable.Master.Client.Timeout", 180);

  m_master_client_ptr = new MasterClient(m_conn_manager_ptr, m_hyperspace_ptr, master_timeout, m_app_queue_ptr);
  if (m_master_client_ptr->initiate_connection(0) != Error::OK) {
    HT_ERROR("Unable to establish connection with Master, exiting...");
    exit(1);
  }
}



