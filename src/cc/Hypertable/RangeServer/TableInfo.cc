/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
bool TableInfo::get_range(RangeT *range, RangePtr &rangePtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  string endRow = range->endRow;

  RangeMapT::iterator iter = m_range_map.find(endRow);

  if (iter == m_range_map.end())
    return false;

  rangePtr = (*iter).second;

  string startRow = rangePtr->start_row();

  if (strcmp(startRow.c_str(), range->startRow))
    return false;

  return true;
}



/**
 * 
 */
void TableInfo::add_range(RangeT *range, RangePtr &rangePtr) {
  boost::mutex::scoped_lock lock(m_mutex);
  RangeMapT::iterator iter = m_range_map.find(range->endRow);
  assert(iter == m_range_map.end());
  m_range_map[range->endRow] = rangePtr;
}


/**
 * 
 */
bool TableInfo::find_containing_range(std::string row, RangePtr &rangePtr) {
  boost::mutex::scoped_lock lock(m_mutex);

  RangeMapT::iterator iter = m_range_map.lower_bound(row);

  if (iter == m_range_map.end())
    return false;

  if (row <= (*iter).second->start_row())
    return false;

  rangePtr = (*iter).second;

  return true;
}

void TableInfo::get_range_vector(std::vector<RangePtr> &range_vec) {
  for (RangeMapT::iterator iter = m_range_map.begin(); iter != m_range_map.end(); iter++)
    range_vec.push_back((*iter).second);
}
