/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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
#include "Common/Config.h"

#include <cassert>
#include <ctime>

#include "AccessGroupGarbageTracker.h"
#include "Global.h"

using namespace Hypertable;
using namespace Config;


AccessGroupGarbageTracker::AccessGroupGarbageTracker()
  : m_elapsed_target(0), m_minimum_elapsed_target(0), m_delete_count(0),
    m_expirable_accumulated(0), m_data_accumulated(0), m_min_ttl(0),
    m_max_ttl(0), m_last_cache_size(-1), m_in_memory(false),
    m_have_max_versions(false), m_need_collection(false) {
  m_minimum_data_target = properties->get_i64("Hypertable.RangeServer.Range.SplitSize") / 10;
  m_data_target = m_minimum_data_target;
  m_last_clear_time = time(0);
}


void AccessGroupGarbageTracker::set_schema(SchemaPtr &schema, Schema::AccessGroup *ag) {

  if (!m_schema || schema->get_generation() >= m_schema->get_generation()) {
    m_have_max_versions = false;
    m_min_ttl = m_max_ttl = 0;
    m_in_memory = ag->in_memory;
    foreach(Schema::ColumnFamily *cf, ag->columns) {
      if (cf->max_versions > 0)
        m_have_max_versions = true;
      if (cf->ttl > 0) {
        if (m_min_ttl == 0) {
          assert(m_max_ttl == 0);
          m_min_ttl = m_max_ttl = cf->ttl;
        }
        else if (cf->ttl < m_min_ttl)
          m_min_ttl = cf->ttl;
        else if (cf->ttl > m_max_ttl)
          m_max_ttl = cf->ttl;
      }
    }
    m_schema = schema;
  }

  m_minimum_elapsed_target = m_elapsed_target = (m_min_ttl > 0) ? (m_min_ttl/10) : 0;

}

void AccessGroupGarbageTracker::clear(time_t now) {
  m_delete_count = 0;
  m_data_accumulated = 0;
  m_expirable_accumulated = 0;
  m_last_clear_time = (now == 0) ? time(0) : now;
  m_last_cache_size = -1;
}


bool AccessGroupGarbageTracker::check_needed(int64_t cached_data, time_t now) {
  if (now == 0)
    now = time(0);
  if (((m_have_max_versions || m_delete_count > 0) &&
       m_data_accumulated >= m_data_target) ||
      ((m_expirable_accumulated+cached_data) >= m_minimum_data_target &&
       m_min_ttl > 0 && (now-m_last_clear_time) >= m_elapsed_target))
    return true;
  return false;
}


bool AccessGroupGarbageTracker::check_needed(uint32_t additional_deletes,
                                             int64_t cached_data,
                                             time_t now) {
  int64_t additional_data = cached_data;
  if (m_in_memory) {
    if (m_last_cache_size == -1)
      m_last_cache_size = cached_data;
    additional_data = cached_data - m_last_cache_size;
  }
  if (((m_have_max_versions || (m_delete_count+additional_deletes) > 0)
       && additional_data > 0
       && (m_data_accumulated+additional_data) >= m_data_target) ||
      ((m_expirable_accumulated+cached_data) >= m_minimum_data_target 
       && m_min_ttl > 0 
       && (now-m_last_clear_time) >= m_elapsed_target))
    return true;
  return false;
}


void AccessGroupGarbageTracker::set_garbage_stats(int64_t total, int64_t valid, time_t now) {
  int64_t new_data_target;
  int32_t new_elapsed_target;
  double garbage_pct = ((double)(total-valid)/(double)total)*100.0;
  m_need_collection = garbage_pct >= (double)Global::access_group_garbage_compaction_threshold;

  /**
   * recompute DATA target:
   *   data_accumulated/garbage_percentage = NEW_DATA_TARGET/threshold
   */
  if (garbage_pct > 0)
    new_data_target = (int64_t)((double)(m_data_accumulated * Global::access_group_garbage_compaction_threshold) / garbage_pct);
  else
    new_data_target = m_data_target * 2;

  if (new_data_target < m_minimum_data_target)
    m_data_target = m_minimum_data_target;
  else if (new_data_target > (m_data_target*2))
    m_data_target *= 2;
  else
    m_data_target = new_data_target;

  /**
   * recompute ELAPSED target:
   *   elapsed_time/garbage_percentage = NEW_ELAPSED_TIME_TARGET/threshold
   */
  if (now == 0)
    now = time(0);
  if (garbage_pct > 0)
    new_elapsed_target = (int32_t)((double)((now-m_last_clear_time) * Global::access_group_garbage_compaction_threshold) / garbage_pct);
  else
    new_elapsed_target = m_elapsed_target * 2;

  if (new_elapsed_target < m_minimum_elapsed_target)
    m_elapsed_target = m_minimum_elapsed_target;
  else if (new_elapsed_target > (m_elapsed_target*2))
    m_elapsed_target *= 2;
  else
    m_elapsed_target = new_elapsed_target;
  
}
