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

#include <boost/algorithm/string.hpp>

#include "AsyncComm/Comm.h"
#include "AsyncComm/ReactorFactory.h"

#include "Common/Init.h"
#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/ScopeGuard.h"
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

  if (!properties)
    init_with_policy<DefaultCommPolicy>(0, 0);

  m_props = new Properties(config_file, file_desc());
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

void Client::create_namespace(const String &name, Namespace *base, bool create_intermediate) {

  String full_name;
  String sub_name = name;
  int flags=NameIdMapper::IS_NAMESPACE;

  if(create_intermediate)
    flags |= NameIdMapper::CREATE_INTERMEDIATE;

  if (base != NULL) {
    full_name = base->get_name() + '/';
  }
  full_name += sub_name;

  Namespace::canonicalize(&full_name);
  m_master_client->create_namespace(full_name, flags);
}

NamespacePtr Client::open_namespace(const String &name, Namespace *base) {
  String full_name;
  String sub_name = name;

  Namespace::canonicalize(&sub_name);

  if (base != NULL) {
    full_name = base->get_name() + '/';
  }

  full_name += sub_name;

  return m_namespace_cache->get(full_name);
}

bool Client::exists_namespace(const String &name, Namespace *base) {
  String id;
  bool is_namespace = false;
  String full_name;
  String sub_name = name;

  Namespace::canonicalize(&sub_name);

  if (base != NULL) {
    full_name = base->get_name() + '/';
  }

  full_name += sub_name;

  if (!m_namemap->name_to_id(full_name, id, &is_namespace) || !is_namespace)
    return false;

  // TODO: issue 11

  String namespace_file = m_toplevel_dir + "/tables/" + id;

  try {
    return m_hyperspace->exists(namespace_file);
  }
  catch(Exception &e) {
    HT_THROW(e.code(), e.what());
  }
}

void Client::drop_namespace(const String &name, Namespace *base, bool if_exists) {
  String full_name;
  String sub_name = name;

  Namespace::canonicalize(&sub_name);

  if (base != NULL) {
    full_name = base->get_name() + '/';
  }

  full_name += sub_name;

  m_namespace_cache->remove(full_name);
  m_master_client->drop_namespace(full_name, if_exists);
}

Hyperspace::SessionPtr& Client::get_hyperspace_session()
{
  return m_hyperspace;
}

void Client::close() {
  HT_WARN("close() is no longer supported");
}

void Client::shutdown() {
  m_master_client->shutdown();
}


HqlInterpreter *Client::create_hql_interpreter(bool immutable_namespace) {
  return new HqlInterpreter(this, this->m_conn_manager, immutable_namespace);
}

// ------------- PRIVATE METHODS -----------------
void Client::initialize() {
  uint32_t wait_time, remaining;
  uint32_t interval=5000;

  m_toplevel_dir = m_props->get_str("Hypertable.Directory");
  boost::trim_if(m_toplevel_dir, boost::is_any_of("/"));
  m_toplevel_dir = String("/") + m_toplevel_dir;

  m_comm = Comm::instance();
  m_conn_manager = new ConnectionManager(m_comm);

  if (m_timeout_ms == 0)
    m_timeout_ms = m_props->get_i32("Hypertable.Request.Timeout");

  m_hyperspace_reconnect = m_props->get_bool("Hyperspace.Session.Reconnect");

  m_hyperspace = new Hyperspace::Session(m_comm, m_props);

  m_namemap = new NameIdMapper(m_hyperspace, m_toplevel_dir);

  m_refresh_schema = m_props->get_bool("Hypertable.Client.RefreshSchema");

  Timer timer(m_timeout_ms, true);

  remaining = timer.remaining();
  wait_time = (remaining < interval) ? remaining : interval;

  while (!m_hyperspace->wait_for_connection(wait_time)) {

    if (timer.expired())
      HT_THROW_(Error::CONNECT_ERROR_HYPERSPACE);

    cout << "Waiting for connection to Hyperspace..." << endl;

    remaining = timer.remaining();
    wait_time = (remaining < interval) ? remaining : interval;
  }

  m_app_queue = new ApplicationQueue(m_props->
                                     get_i32("Hypertable.Client.Workers"));
  m_master_client = new MasterClient(m_conn_manager, m_hyperspace, m_toplevel_dir,
                                     m_timeout_ms, m_app_queue);

  m_master_client->initiate_connection(0);

  if (!m_master_client->wait_for_connection(timer))
    HT_THROW(Error::REQUEST_TIMEOUT, "Waiting for Master connection");

  m_range_locator = new RangeLocator(m_props, m_conn_manager, m_hyperspace,
                                     m_timeout_ms);
  m_table_cache = new  TableCache(m_props, m_range_locator, m_conn_manager, m_hyperspace,
                                  m_app_queue, m_namemap, m_timeout_ms);
  m_namespace_cache = new NamespaceCache(m_props, m_range_locator, m_conn_manager, m_hyperspace,
                                         m_app_queue, m_namemap, m_master_client,
                                         m_table_cache, m_timeout_ms);
}
