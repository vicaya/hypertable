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

#include "Common/Logger.h"

#include "TableInfo.h"

using namespace Hypertable;


/**
 *
 */
TableInfo::TableInfo(MasterClientPtr &master_client_ptr, TableIdentifierT *identifier, SchemaPtr &schemaPtr) : m_mutex(), m_master_client_ptr(master_client_ptr), m_schema(schemaPtr) { 
  Copy(*identifier, m_identifier);
  return;
}


/**
 */
TableInfo::~TableInfo() {
  Free(m_identifier);
}


void TableInfo::dump_range_table() {
  boost::mutex::scoped_lock lock(m_mutex);
  for (RangeMapT::iterator iter = m_range_map.begin(); iter != m_range_map.end(); iter++) {
    cout << m_identifier.name << "[" << (*iter).second->start_row() << ".." << (*iter).second->end_row() << "]" << endl;
  }
}

/**
 * 
 */
bool TableInfo::get_range(RangeT *range, RangePtr &range_ptr) {
  boost::mutex::scoped_lock lock(m_mutex);
  string endRow = range->endRow;

  RangeMapT::iterator iter = m_range_map.find(endRow);

  if (iter == m_range_map.end())
    return false;

  range_ptr = (*iter).second;

  string startRow = range_ptr->start_row();

  if (strcmp(startRow.c_str(), range->startRow))
    return false;

  return true;
}


/**
 * 
 */
bool TableInfo::remove_range(RangeT *range, RangePtr &range_ptr) {
  boost::mutex::scoped_lock lock(m_mutex);
  string endRow = range->endRow;

  RangeMapT::iterator iter = m_range_map.find(endRow);

  if (iter == m_range_map.end())
    return false;

  range_ptr = (*iter).second;

  string startRow = range_ptr->start_row();

  if (strcmp(startRow.c_str(), range->startRow))
    return false;

  m_range_map.erase(iter);

  return true;
}



/**
 * 
 */
void TableInfo::add_range(RangePtr &range_ptr) {
  boost::mutex::scoped_lock lock(m_mutex);
  RangeMapT::iterator iter = m_range_map.find(range_ptr->end_row());
  assert(iter == m_range_map.end());
  m_range_map[range_ptr->end_row()] = range_ptr;
}


/**
 * 
 */
bool TableInfo::find_containing_range(std::string row, RangePtr &range_ptr) {
  boost::mutex::scoped_lock lock(m_mutex);

  RangeMapT::iterator iter = m_range_map.lower_bound(row);

  if (iter == m_range_map.end())
    return false;

  if (row <= (*iter).second->start_row())
    return false;

  range_ptr = (*iter).second;

  return true;
}


void TableInfo::get_range_vector(std::vector<RangePtr> &range_vec) {
  for (RangeMapT::iterator iter = m_range_map.begin(); iter != m_range_map.end(); iter++)
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
