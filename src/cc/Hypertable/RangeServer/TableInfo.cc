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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "Common/Logger.h"

#include "TableInfo.h"

using namespace std;
using namespace Hypertable;

/**
 *
 */
TableInfo::TableInfo(MasterClientPtr &master_client_ptr, TableIdentifier *identifier, SchemaPtr &schemaPtr) : m_mutex(), m_master_client_ptr(master_client_ptr), m_identifier(identifier), m_schema(schemaPtr) {
  return;
}


void TableInfo::dump_range_table() {
  boost::mutex::scoped_lock lock(m_mutex);
  for (RangeMap::iterator iter = m_range_map.begin(); iter != m_range_map.end(); iter++) {
    cout << m_identifier.name << "[" << (*iter).second->start_row() << ".." << (*iter).second->end_row() << "]" << endl;
  }
}

/**
 *
 */
bool TableInfo::get_range(RangeSpec *range, RangePtr &range_ptr) {
  boost::mutex::scoped_lock lock(m_mutex);
  string end_row = range->end_row;

  RangeMap::iterator iter = m_range_map.find(end_row);

  if (iter == m_range_map.end())
    return false;

  range_ptr = (*iter).second;

  string start_row = range_ptr->start_row();

  if (strcmp(start_row.c_str(), range->start_row))
    return false;

  return true;
}


/**
 *
 */
bool TableInfo::remove_range(RangeSpec *range, RangePtr &range_ptr) {
  boost::mutex::scoped_lock lock(m_mutex);
  string end_row = range->end_row;

  RangeMap::iterator iter = m_range_map.find(end_row);

  if (iter == m_range_map.end())
    return false;

  range_ptr = (*iter).second;

  string start_row = range_ptr->start_row();

  if (strcmp(start_row.c_str(), range->start_row))
    return false;

  m_range_map.erase(iter);

  return true;
}



/**
 *
 */
void TableInfo::add_range(RangePtr &range_ptr) {
  boost::mutex::scoped_lock lock(m_mutex);
  RangeMap::iterator iter = m_range_map.find(range_ptr->end_row());
  assert(iter == m_range_map.end());
  m_range_map[range_ptr->end_row()] = range_ptr;
}


/**
 *
 */
bool TableInfo::find_containing_range(std::string row, RangePtr &range_ptr) {
  boost::mutex::scoped_lock lock(m_mutex);

  RangeMap::iterator iter = m_range_map.lower_bound(row);

  if (iter == m_range_map.end())
    return false;

  if (row <= (*iter).second->start_row())
    return false;

  range_ptr = (*iter).second;

  return true;
}


void TableInfo::get_range_vector(std::vector<RangePtr> &range_vec) {
  boost::mutex::scoped_lock lock(m_mutex);
  for (RangeMap::iterator iter = m_range_map.begin(); iter != m_range_map.end(); iter++)
    range_vec.push_back((*iter).second);
}


void TableInfo::clear() {
  boost::mutex::scoped_lock lock(m_mutex);
  m_range_map.clear();
}


/**
 *
 */
TableInfo *TableInfo::create_shallow_copy() {
  boost::mutex::scoped_lock lock(m_mutex);
  return new TableInfo(m_master_client_ptr, &m_identifier, m_schema);
}
