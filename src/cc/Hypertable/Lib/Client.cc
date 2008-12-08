/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
#include <cassert>
#include <cstdlib>

extern "C" {
#include <poll.h>
}

#include "AsyncComm/Comm.h"
#include "AsyncComm/ReactorFactory.h"

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Timer.h"

#include "Hyperspace/DirEntry.h"
#include "Hypertable/Lib/Config.h"

#include "Client.h"
#include "HqlCommandInterpreter.h"

using namespace std;
using namespace Hypertable;
using namespace Hyperspace;
using namespace Config;


Client::Client(const String &install_dir, const String &config_file,
               uint32_t default_timeout_ms)
  : m_timeout_ms(default_timeout_ms), m_install_dir(install_dir) {
  ScopedRecLock lock(rec_mutex);

  if (!properties) {
    init_with_policy<DefaultCommPolicy>(0, 0);
    m_props = new Properties();
    m_props->load(config_file, file_desc());
  }
  initialize();
}


Client::Client(const String &install_dir, uint32_t default_timeout_ms)
  : m_timeout_ms(default_timeout_ms), m_install_dir(install_dir) {
  ScopedRecLock lock(rec_mutex);

  if (!properties)
    init_with_policy<DefaultCommPolicy>(0, 0);

  if (m_install_dir.empty())
    m_install_dir = System::install_dir;

  m_props = properties;
  initialize();
}

/**
 */
Client::~Client() {
  m_range_locator = 0;
  m_master_client = 0;
  m_app_queue = 0;
  m_hyperspace = 0;
  m_conn_manager = 0;
}



void Client::create_table(const String &name, const String &schema) {
  int error;

  if ((error = m_master_client->create_table(name.c_str(), schema.c_str()))
      != Error::OK)
    HT_THROW_(error);
}


Table *Client::open_table(const String &name) {
  TableCache::iterator it = m_table_cache.find(name);

  if (it != m_table_cache.end()) {
    if (!it->second->not_found())
      return it->second.get();

    m_table_cache.erase(it);
  }
  Table *table = new Table(m_range_locator, m_conn_manager, m_hyperspace, name,
                           m_timeout_ms);
  m_table_cache.insert(make_pair(name, table));
  return table;
}


uint32_t Client::get_table_id(const String &name) {
  // TODO: issue 11
  String table_file("/hypertable/tables/"); table_file += name;
  DynamicBuffer value_buf(0);
  Hyperspace::HandleCallbackPtr null_handle_callback;
  uint64_t handle;
  uint32_t uval;

  handle = m_hyperspace->open(table_file, OPEN_FLAG_READ, null_handle_callback);

  // Get the 'table_id' attribute. TODO use attr_get_i32
  m_hyperspace->attr_get(handle, "table_id", value_buf);

  m_hyperspace->close(handle);

  uval = (uint32_t)atoi((const char *)value_buf.base);

  return uval;
}


String Client::get_schema(const String &name) {
  int error;
  String schema;

  if ((error = m_master_client->get_schema(name.c_str(), schema)) != Error::OK)
    HT_THROW_(error);

  return schema;
}


void Client::get_tables(std::vector<String> &tables) {
  uint64_t handle;
  HandleCallbackPtr null_handle_callback;
  std::vector<Hyperspace::DirEntry> listing;

  handle = m_hyperspace->open("/hypertable/tables", OPEN_FLAG_READ,
                              null_handle_callback);

  m_hyperspace->readdir(handle, listing);

  for (size_t i=0; i<listing.size(); i++) {
    if (!listing[i].is_dir)
      tables.push_back(listing[i].name);
  }

  m_hyperspace->close(handle);
}


void Client::drop_table(const String &name, bool if_exists) {
  int error;

  // remove it from cache
  TableCache::iterator it = m_table_cache.find(name);

  if (it != m_table_cache.end())
    m_table_cache.erase(it);

  if ((error = m_master_client->drop_table(name.c_str(), if_exists))
      != Error::OK)
    HT_THROW_(error);
}


void Client::shutdown() {
  int error;

  if ((error = m_master_client->shutdown()) != Error::OK)
    HT_THROW_(error);
}


HqlInterpreter *Client::create_hql_interpreter() {
  return new HqlInterpreter(this);
}


// ------------- PRIVATE METHODS -----------------

void Client::initialize() {
  m_comm = Comm::instance();
  m_conn_manager = new ConnectionManager(m_comm);

  if (m_timeout_ms == 0)
    m_timeout_ms = m_props->get_i32("Hypertable.Client.Timeout");

  m_hyperspace = new Hyperspace::Session(m_comm, m_props);

  Timer timer(m_timeout_ms, true);

  while (!m_hyperspace->wait_for_connection(3000)) {
    if (timer.expired())
      HT_THROW_(Error::CONNECT_ERROR_HYPERSPACE);

    cout << "Waiting for connection to Hyperspace..." << endl;
    poll(0, 0, System::rand32() % 5000);
  }
  m_app_queue = new ApplicationQueue(1);
  m_master_client = new MasterClient(m_conn_manager, m_hyperspace, m_timeout_ms,
                                     m_app_queue);

  if (m_master_client->initiate_connection(0) != Error::OK) {
    HT_ERROR("Unable to establish connection with Master, exiting...");
    exit(1);
  }

  m_range_locator = new RangeLocator(m_props, m_conn_manager, m_hyperspace,
                                     m_timeout_ms);
}
