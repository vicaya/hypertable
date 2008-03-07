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

#include "TableInfoMap.h"

using namespace Hypertable;

TableInfoMap::~TableInfoMap() {
  m_map.clear();
}


bool TableInfoMap::get(std::string name, TableInfoPtr &info) {
  boost::mutex::scoped_lock lock(m_mutex);
  TableInfoMapT::iterator iter = m_map.find(name);
  if (iter == m_map.end())
    return false;
  info = (*iter).second;
  return true;
}


void TableInfoMap::set(std::string name, TableInfoPtr &info) {
  boost::mutex::scoped_lock lock(m_mutex);
  TableInfoMapT::iterator iter = m_map.find(name);
  if (iter != m_map.end())
    m_map.erase(iter);
  m_map[name] = info;
}


bool TableInfoMap::remove(std::string name, TableInfoPtr &info) {
  boost::mutex::scoped_lock lock(m_mutex);
  TableInfoMapT::iterator iter = m_map.find(name);
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
