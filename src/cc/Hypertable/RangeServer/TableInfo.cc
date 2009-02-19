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
#include "Common/Logger.h"

#include "TableInfo.h"

using namespace std;
using namespace Hypertable;


TableInfo::TableInfo(MasterClientPtr &master_client,
                     const TableIdentifier *identifier, SchemaPtr &schema)
    : m_master_client(master_client),
      m_identifier(*identifier), m_schema(schema) {
}


bool TableInfo::remove(const String &end_row) {
  ScopedLock lock(m_mutex);
  RangeMap::iterator iter = m_range_map.find(end_row);

  if (iter == m_range_map.end())
    return false;

  m_range_map.erase(iter);

  return true;
}


bool
TableInfo::change_end_row(const String &old_end_row,
                          const String &new_end_row) {
  ScopedLock lock(m_mutex);
  RangeMap::iterator iter = m_range_map.find(old_end_row);

  if (iter == m_range_map.end())
    return false;

  RangePtr range = (*iter).second;

  m_range_map.erase(iter);

  iter = m_range_map.find(new_end_row);

  HT_ASSERT(iter == m_range_map.end());

  m_range_map[new_end_row] = range;

  return true;
}


void TableInfo::dump_range_table() {
  ScopedLock lock(m_mutex);
  for (RangeMap::iterator iter = m_range_map.begin();
       iter != m_range_map.end(); ++iter) {
    cout << m_identifier.name << "[" << (*iter).second->start_row() << ".."
         << (*iter).second->end_row() << "]" << endl;
  }
}


bool TableInfo::get_range(const RangeSpec *range_spec, RangePtr &range) {
  ScopedLock lock(m_mutex);
  string end_row = range_spec->end_row;

  RangeMap::iterator iter = m_range_map.find(end_row);

  if (iter == m_range_map.end()) {
    HT_DEBUG_OUT <<"TableInfo couldn't find end row ("<< end_row <<")"<< HT_END;

    for (iter = m_range_map.begin(); iter != m_range_map.end(); ++iter)
      HT_DEBUG_OUT <<"TableInfo map: "<< (*iter).first << HT_END;

    return false;
  }

  range = (*iter).second;

  string start_row = range->start_row();

  if (strcmp(start_row.c_str(), range_spec->start_row)) {
    HT_INFO_OUT <<"TableInfo start row mismatch '" << start_row << "' != '"
                << range_spec->start_row << "'" << HT_END;
    range = 0;
    return false;
  }

  return true;
}


bool TableInfo::remove_range(const RangeSpec *range_spec, RangePtr &range) {
  ScopedLock lock(m_mutex);
  string end_row = range_spec->end_row;

  RangeMap::iterator iter = m_range_map.find(end_row);

  if (iter == m_range_map.end())
    return false;

  range = (*iter).second;

  string start_row = range->start_row();

  if (strcmp(start_row.c_str(), range_spec->start_row))
    return false;

  m_range_map.erase(iter);

  return true;
}


void TableInfo::add_range(RangePtr &range) {
  ScopedLock lock(m_mutex);
  RangeMap::iterator iter = m_range_map.find(range->end_row());
  assert(iter == m_range_map.end());
  m_range_map[range->end_row()] = range;
}


bool
TableInfo::find_containing_range(const String &row, RangePtr &range,
                                 String &start_row, String &end_row) {
  ScopedLock lock(m_mutex);

  RangeMap::iterator iter = m_range_map.lower_bound(row);

  if (iter == m_range_map.end())
    return false;

  start_row = (*iter).second->start_row();
  end_row = (*iter).second->end_row();

  if (row <= start_row)
    return false;

  range = (*iter).second;

  return true;
}


void TableInfo::get_range_vector(std::vector<RangePtr> &range_vec) {
  ScopedLock lock(m_mutex);
  for (RangeMap::iterator iter = m_range_map.begin();
       iter != m_range_map.end(); ++iter)
    range_vec.push_back((*iter).second);
}


void TableInfo::clear() {
  ScopedLock lock(m_mutex);
  m_range_map.clear();
}

void TableInfo::update_schema(SchemaPtr &schema_ptr) {
  ScopedLock lock(m_mutex);
  // Update individual ranges
  for (RangeMap::iterator iter = m_range_map.begin();
       iter != m_range_map.end(); ++iter) {
    iter->second->update_schema(schema_ptr);
  }
  // update table info
  m_schema = schema_ptr;
}




/**
 *
 */
TableInfo *TableInfo::create_shallow_copy() {
  ScopedLock lock(m_mutex);
  return new TableInfo(m_master_client, &m_identifier, m_schema);
}
