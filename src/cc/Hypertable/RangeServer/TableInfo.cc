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

  if (iter == m_range_map.end() || iter->second == 0) {
    HT_INFOF("Unable to remove %s[end=%s] from TableInfo because non-existant",
             m_identifier.id, end_row.c_str());
    return false;
  }

  HT_INFOF("Removing %s[end=%s] from TableInfo",
           m_identifier.id, end_row.c_str());

  m_range_map.erase(iter);

  return true;
}


bool
TableInfo::change_end_row(const String &old_end_row,
                          const String &new_end_row) {
  ScopedLock lock(m_mutex);
  RangeMap::iterator iter = m_range_map.find(old_end_row);

  if (iter == m_range_map.end() || iter->second == 0) {
    HT_ERRORF("%p: Problem changing old end row '%s' to '%s'", (void *)this,
              old_end_row.c_str(), new_end_row.c_str());
    for (m_range_map.begin(); iter != m_range_map.end(); ++iter) {
      HT_INFOF("%p: %s -> %s[%s..%s]", (void *)this,
               (*iter).first.c_str(),
               m_identifier.id,
               (*iter).second->start_row().c_str(),
               (*iter).second->end_row().c_str());
    }
    return false;
  }

  RangePtr range = (*iter).second;

  HT_INFOF("Changing end row %s removing old row '%s'",
           m_identifier.id, old_end_row.c_str());

  m_range_map.erase(iter);

  iter = m_range_map.find(new_end_row);

  HT_ASSERT(iter == m_range_map.end());

  HT_INFOF("Changing end row %s adding new row '%s'",
           m_identifier.id, new_end_row.c_str());

  m_range_map[new_end_row] = range;

  return true;
}


void TableInfo::dump_range_table() {
  ScopedLock lock(m_mutex);
  for (RangeMap::iterator iter = m_range_map.begin();
       iter != m_range_map.end(); ++iter) {
    if (iter->second != 0)
      HT_INFOF("%p: %s -> %s[%s..%s]", (void *)this,
               (*iter).first.c_str(), m_identifier.id,
               (*iter).second->start_row().c_str(),
               (*iter).second->end_row().c_str());
  }
}


bool TableInfo::get_range(const RangeSpec *range_spec, RangePtr &range) {
  ScopedLock lock(m_mutex);
  string end_row = range_spec->end_row;

  RangeMap::iterator iter = m_range_map.find(end_row);

  if (iter == m_range_map.end() || iter->second == 0) {
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

bool TableInfo::has_range(const RangeSpec *range_spec) {
  ScopedLock lock(m_mutex);
  string end_row = range_spec->end_row;
  if (m_range_map.find(end_row) == m_range_map.end())
    return false;
  return true;
}


bool TableInfo::remove_range(const RangeSpec *range_spec, RangePtr &range) {
  ScopedLock lock(m_mutex);
  string end_row = range_spec->end_row;

  RangeMap::iterator iter = m_range_map.find(end_row);

  if (iter == m_range_map.end() || iter->second == 0) {
    HT_INFOF("Problem removing range %s[end=%s] from TableInfo, end row not found",
             m_identifier.id, end_row.c_str());
    return false;
  }

  range = (*iter).second;

  string start_row = range->start_row();

  if (strcmp(start_row.c_str(), range_spec->start_row)) {
    HT_INFOF("Problem removing range %s[end=%s] from TableInfo, start row mismatch %s != %s",
             m_identifier.id, end_row.c_str(),
             start_row.c_str(), range_spec->start_row);
    return false;
  }

  HT_INFOF("Removing range %s[end=%s] from TableInfo",
           m_identifier.id, end_row.c_str());

  m_range_map.erase(iter);

  return true;
}


void TableInfo::stage_range(const RangeSpec *range_spec) {
  ScopedLock lock(m_mutex);
  RangeMap::iterator iter = m_range_map.find(range_spec->end_row);
  assert(iter == m_range_map.end());
  HT_INFOF("Staging range %s[%s..%s] to TableInfo",
           m_identifier.id, range_spec->start_row,
           range_spec->end_row);
  m_range_map[range_spec->end_row] = 0;
}

void TableInfo::unstage_range(const RangeSpec *range_spec) {
  ScopedLock lock(m_mutex);
  RangeMap::iterator iter = m_range_map.find(range_spec->end_row);
  assert(iter != m_range_map.end());
  assert(iter->second == 0);
  HT_INFOF("Unstaging range %s[%s..%s] to TableInfo",
           m_identifier.id, range_spec->start_row,
           range_spec->end_row);
  m_range_map.erase(iter);
}


void TableInfo::add_staged_range(RangePtr &range) {
  ScopedLock lock(m_mutex);
  RangeMap::iterator iter = m_range_map.find(range->end_row());
  assert(iter != m_range_map.end());
  assert(iter->second == 0);
  HT_INFOF("Adding range %s to TableInfo end row = %s",
           range->get_name().c_str(),
           range->end_row().c_str());
  m_range_map[range->end_row()] = range;
}


void TableInfo::add_range(RangePtr &range) {
  ScopedLock lock(m_mutex);
  RangeMap::iterator iter = m_range_map.find(range->end_row());
  assert(iter == m_range_map.end());
  HT_INFOF("Adding range %s to TableInfo end row = %s",
           range->get_name().c_str(),
           range->end_row().c_str());
  m_range_map[range->end_row()] = range;
}


bool
TableInfo::find_containing_range(const String &row, RangePtr &range,
                                 String &start_row, String &end_row) {
  ScopedLock lock(m_mutex);

  RangeMap::iterator iter = m_range_map.lower_bound(row);

  if (iter == m_range_map.end() || (*iter).second == 0)
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
       iter != m_range_map.end(); ++iter) {
    if (iter->second != 0)
      range_vec.push_back(iter->second);
  }
}


int32_t TableInfo::get_range_count() {
  ScopedLock lock(m_mutex);
  return m_range_map.size();
}


void TableInfo::clear() {
  ScopedLock lock(m_mutex);
  HT_INFOF("Clearing map for table %s",
           m_identifier.id);
  m_range_map.clear();
}

void TableInfo::update_schema(SchemaPtr &schema_ptr) {
  ScopedLock lock(m_mutex);
  // Update individual ranges
  for (RangeMap::iterator iter = m_range_map.begin();
       iter != m_range_map.end(); ++iter) {
    if (iter->second != 0)
      iter->second->update_schema(schema_ptr);
  }
  // update table info
  m_schema = schema_ptr;
}
