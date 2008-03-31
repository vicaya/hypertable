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

#include "Global.h"
#include "TableInfoMap.h"

using namespace Hypertable;

TableInfoMap::~TableInfoMap() {
  m_map.clear();
}


bool TableInfoMap::get(uint32_t id, TableInfoPtr &info) {
  boost::mutex::scoped_lock lock(m_mutex);
  TableInfoMapT::iterator iter = m_map.find(id);
  if (iter == m_map.end())
    return false;
  info = (*iter).second;
  return true;
}


void TableInfoMap::set(uint32_t id, TableInfoPtr &info) {
  boost::mutex::scoped_lock lock(m_mutex);
  TableInfoMapT::iterator iter = m_map.find(id);
  if (iter != m_map.end())
    m_map.erase(iter);
  m_map[id] = info;
}


bool TableInfoMap::remove(uint32_t id, TableInfoPtr &info) {
  boost::mutex::scoped_lock lock(m_mutex);
  TableInfoMapT::iterator iter = m_map.find(id);
  if (iter == m_map.end())
    return false;
  info = (*iter).second;
  m_map.erase(iter);
  return true;
}


void TableInfoMap::get_all(std::vector<TableInfoPtr> &tv) {
  boost::mutex::scoped_lock lock(m_mutex);
  for (TableInfoMapT::iterator iter = m_map.begin(); iter != m_map.end(); iter++)
    tv.push_back((*iter).second);
}


void TableInfoMap::clear() {
  boost::mutex::scoped_lock lock(m_mutex);
  m_map.clear();
}


void TableInfoMap::clear_ranges() {
  boost::mutex::scoped_lock lock(m_mutex);
  for (TableInfoMapT::iterator iter = m_map.begin(); iter != m_map.end(); iter++)
    (*iter).second->clear();
}


void TableInfoMap::atomic_merge(TableInfoMapPtr &table_info_map_ptr, CommitLogPtr &replay_log_ptr) {
  boost::mutex::scoped_lock lock(m_mutex);
  TableInfoMapT::iterator from_iter, to_iter;
  uint32_t table_id;
  int error;

  for (from_iter = table_info_map_ptr->m_map.begin(); from_iter != table_info_map_ptr->m_map.end(); from_iter++) {

    table_id = (*from_iter).second->get_id();

    to_iter = m_map.find(table_id);

    if (to_iter == m_map.end())
      m_map[table_id] = (*from_iter).second;
    else {
      std::vector<RangePtr> range_vec;
      (*from_iter).second->get_range_vector(range_vec);
      for (size_t i=0; i<range_vec.size(); i++)
	(*to_iter).second->add_range(range_vec[i]);
    }
  }

  table_info_map_ptr->clear();

  if ((error = Global::log->link_log(0, replay_log_ptr->get_log_dir().c_str(), Global::log->get_timestamp())) != Error::OK)
    throw Exception(error, std::string("Problem linking replay commit log '") + replay_log_ptr->get_log_dir() + "'");
}
