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


bool TableInfoMap::get(uint32_t id, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(id);
  if (iter == m_map.end())
    return false;
  info = (*iter).second;
  return true;
}

void TableInfoMap::get(const TableIdentifier *table, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(table->id);
  if (iter == m_map.end())
    HT_THROWF(Error::RANGESERVER_TABLE_NOT_FOUND, "unknown table id=%d '%s'",
              table->id, table->name);
  info = (*iter).second;
  if (strcmp(table->name, info->get_name().c_str()))
    HT_THROWF(Error::RANGESERVER_UNEXPECTED_TABLE_ID,
              "Request thinks table with id %d is '%s', server thinks it's '%s'",
              table->id, table->name, info->get_name().c_str());
}


void TableInfoMap::set(uint32_t id, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(id);
  if (iter != m_map.end())
    m_map.erase(iter);
  m_map[id] = info;
}


bool TableInfoMap::remove(uint32_t id, TableInfoPtr &info) {
  ScopedLock lock(m_mutex);
  InfoMap::iterator iter = m_map.find(id);
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

    range_vec.clear();
    if (to_iter == m_map.end()) {
      m_map[ (*from_iter).first ] = (*from_iter).second;
      (*from_iter).second->get_range_vector(range_vec);
      for (size_t i=0; i<range_vec.size(); i++) {
        if (range_vec[i]->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
            range_vec[i]->get_state() == RangeState::SPLIT_SHRUNK) {
          if (!range_vec[i]->test_and_set_maintenance())
            Global::maintenance_queue->add(new MaintenanceTaskSplit(
                                           range_vec[i]));
        }
      }
    }
    else {
      (*from_iter).second->get_range_vector(range_vec);
      for (size_t i=0; i<range_vec.size(); i++) {
        (*to_iter).second->add_range(range_vec[i]);
        if (range_vec[i]->get_state() == RangeState::SPLIT_LOG_INSTALLED ||
            range_vec[i]->get_state() == RangeState::SPLIT_SHRUNK) {
          if (!range_vec[i]->test_and_set_maintenance())
            Global::maintenance_queue->add(new MaintenanceTaskSplit(
                                           range_vec[i]));
        }
      }
    }

  }

  table_info_map_ptr->clear();
}



void TableInfoMap::dump() {
  InfoMap::iterator table_iter;
  std::vector<RangePtr> range_vec;

  for (table_iter = m_map.begin(); table_iter != m_map.end(); ++table_iter)
    (*table_iter).second->get_range_vector(range_vec);

  std::cout << std::endl;
  for (size_t i=0; i<range_vec.size(); i++)
    std::cout << "tidump " << range_vec[i]->get_name() << "\n";
  std::cout << std::flush;
}
