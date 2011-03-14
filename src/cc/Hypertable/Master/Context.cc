/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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

#include "Context.h"
#include "Operation.h"

using namespace Hypertable;
using namespace std;

Context::~Context() {
  if (hyperspace && master_file_handle > 0) {
    hyperspace->close(master_file_handle);
    master_file_handle = 0;
  }
}

bool Context::connect_server(RangeServerConnectionPtr &rsc, const String &hostname,
                             InetAddr local_addr, InetAddr public_addr) {
  ScopedLock lock(mutex);
  LocationIndex &hash_index = m_server_list.get<1>();
  LocationIndex::iterator iter;
  bool retval = false;
  bool notify = false;

  comm->set_alias(local_addr, public_addr);
  comm->add_proxy(rsc->location(), public_addr);
  HT_INFOF("Registered proxy %s", rsc->location().c_str());

  if (rsc->connect(hostname, local_addr, public_addr)) {
    conn_count++;
    if (conn_count == 1)
      notify = true;
    retval = true;
  }

  // Remove this connection if already exists
  iter = hash_index.find(rsc->location());
  if (iter != hash_index.end())
    hash_index.erase(iter);

  // Add it (or re-add it)
  pair<Sequence::iterator, bool> insert_result = m_server_list.push_back( RangeServerConnectionEntry(rsc) );
  HT_ASSERT(insert_result.second);
  if (m_server_list.size() == 1)
    m_server_list_iter = m_server_list.begin();

  if (notify)
    cond.notify_all();

  return retval;
}

bool Context::disconnect_server(RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  HT_ASSERT(conn_count > 0);
  if (rsc->disconnect()) {
    conn_count--;
    return true;
  }
  return false;
}

void Context::wait_for_server() {
  ScopedLock lock(mutex);
  while (conn_count == 0)
    cond.wait(lock);
}


bool Context::find_server_by_location(const String &location, RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  LocationIndex &hash_index = m_server_list.get<1>();
  LocationIndex::iterator lookup_iter;

  if ((lookup_iter = hash_index.find(location)) == hash_index.end()) {
    rsc = 0;
    return false;
  }
  rsc = lookup_iter->rsc;
  return true;
}


bool Context::find_server_by_hostname(const String &hostname, RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  HostnameIndex &hash_index = m_server_list.get<2>();

  pair<HostnameIndex::iterator, HostnameIndex::iterator> p = hash_index.equal_range(hostname);
  if (p.first != p.second) {
    rsc = p.first->rsc;
    if (++p.first == p.second)
      return true;
    rsc = 0;
  }
  return false;
}



bool Context::find_server_by_public_addr(InetAddr addr, RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  PublicAddrIndex &hash_index = m_server_list.get<3>();
  PublicAddrIndex::iterator lookup_iter;

  if ((lookup_iter = hash_index.find(addr)) == hash_index.end()) {
    rsc = 0;
    return false;
  }
  rsc = lookup_iter->rsc;
  return true;
}


bool Context::find_server_by_local_addr(InetAddr addr, RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);
  LocalAddrIndex &hash_index = m_server_list.get<4>();

  for (pair<LocalAddrIndex::iterator, LocalAddrIndex::iterator> p = hash_index.equal_range(addr);
       p.first != p.second; ++p.first) {
    if (p.first->connected()) {
      rsc = p.first->rsc;
      return true;
    }
  }
  return false;
}


bool Context::next_available_server(RangeServerConnectionPtr &rsc) {
  ScopedLock lock(mutex);

  if (m_server_list.empty())
    return false;

  ServerList::iterator saved_iter = m_server_list_iter;

  do {
    ++m_server_list_iter;
    if (m_server_list_iter == m_server_list.end())
      m_server_list_iter = m_server_list.begin();
    if (m_server_list_iter->rsc->connected()) {
      rsc = m_server_list_iter->rsc;
      return true;
    }
  } while (m_server_list_iter != saved_iter);

  return false;
}

bool Context::reassigned(TableIdentifier *table, RangeSpec &range, String &location) {
  // TBD
  return false;
}


bool Context::is_connected(const String &location) {
  RangeServerConnectionPtr rsc;
  if (find_server_by_location(location, rsc))
    rsc->connected();
  return false;
}


void Context::get_servers(std::vector<RangeServerConnectionPtr> &servers) {
  ScopedLock lock(mutex);
  for (ServerList::iterator iter = m_server_list.begin(); iter != m_server_list.end(); ++iter) {
    if (!iter->removed())
      servers.push_back(iter->rsc);
  }
}

bool Context::in_progress(Operation *operation) {
  ScopedLock lock(mutex);
  return in_progress_ops.count(operation->hash_code()) > 0;
}

bool Context::add_in_progress(Operation *operation) {
  ScopedLock lock(mutex);
  if (in_progress_ops.count(operation->hash_code()) > 0)
    return false;
  in_progress_ops.insert(operation->hash_code());
  return true;
}


void Context::remove_in_progress(Operation *operation) {
  ScopedLock lock(mutex);
  if (in_progress_ops.count(operation->hash_code()) > 0)
    in_progress_ops.erase(operation->hash_code());
}

void Context::clear_in_progress() {
  ScopedLock lock(mutex);
  in_progress_ops.clear();
}
