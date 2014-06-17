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
#include "Global.h"
#include "MaintenanceTaskSplit.h"
#include "TableInfoMap.h"

using namespace Hypertable;

TableInfoMap::~TableInfoMap() {
  m_map.clear();
}


bool TableInfoMap::get(const String &name, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(name);
  if (iter == m_map.end())
    return false;
  info = (*iter).second;
  return true;
}

void TableInfoMap::get(const TableIdentifier *table, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(table->id);
  if (iter == m_map.end())
    HT_THROWF(Error::RANGESERVER_TABLE_NOT_FOUND, "unknown table '%s'",
              table->id);
  info = (*iter).second;
}


void TableInfoMap::set(const String &name, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(name);
  if (iter != m_map.end()) {
    HT_ERRORF("Table %s already exists in map", name.c_str());
    m_map.erase(iter);
  }
  m_map[name] = info;
}


bool TableInfoMap::remove(const String &name, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(name);
  if (iter == m_map.end())
    return false;
  info = (*iter).second;
  m_map.erase(iter);
  return true;
}


void TableInfoMap::get_all(std::vector<TableInfoPtr> &tv) {
  ScopedLock lock(m_mutex);
  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); ++iter)
    tv.push_back((*iter).second);
}

void TableInfoMap::get_range_vector(std::vector<RangePtr> &range_vec) {
  ScopedLock lock(m_mutex);
  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); iter++)
    (*iter).second->get_range_vector(range_vec);
}

int32_t TableInfoMap::get_range_count() {
  ScopedLock lock(m_mutex);
  int32_t count = 0;
  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); iter++)
    count += (*iter).second->get_range_count();
  return count;
}


void TableInfoMap::clear() {
  ScopedLock lock(m_mutex);
  m_map.clear();
}


void TableInfoMap::clear_ranges() {
  ScopedLock lock(m_mutex);
  for (InfoMap::iterator iter = m_map.begin(); iter != m_map.end(); ++iter)
    (*iter).second->clear();
}


void TableInfoMap::merge(TableInfoMapPtr &table_info_map_ptr) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator from_iter, to_iter;
  std::vector<RangePtr> range_vec;

  for (from_iter = table_info_map_ptr->m_map.begin();
       from_iter != table_info_map_ptr->m_map.end(); ++from_iter) {

    to_iter = m_map.find( (*from_iter).first );

    if (to_iter == m_map.end()) {
      m_map[ (*from_iter).first ] = (*from_iter).second;
    }
    else {
      range_vec.clear();
      (*from_iter).second->get_range_vector(range_vec);
      for (size_t i=0; i<range_vec.size(); i++)
        (*to_iter).second->add_range(range_vec[i]);
    }

  }

  table_info_map_ptr->clear();
}



void TableInfoMap::dump() {
  InfoMap::iterator table_iter;
  for (table_iter = m_map.begin(); table_iter != m_map.end(); ++table_iter)
    (*table_iter).second->dump_range_table();
}
