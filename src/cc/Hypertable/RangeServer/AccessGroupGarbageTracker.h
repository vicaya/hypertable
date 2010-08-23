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

#ifndef HYPERTABLE_ACCESSGROUPGARBAGETRACKER_H
#define HYPERTABLE_ACCESSGROUPGARBAGETRACKER_H

extern "C" {
#include <time.h>
}

#include "Hypertable/Lib/Schema.h"

namespace Hypertable {

  class AccessGroupGarbageTracker {
  public:
    AccessGroupGarbageTracker();
    void set_schema(SchemaPtr &schema, Schema::AccessGroup *ag);
    void add_delete_count(int32_t count) { m_delete_count += count; }
    void clear(time_t now=0);
    void accumulate_data(int64_t amount) { m_data_accumulated += amount; }
    void accumulate_expirable(int64_t amount) { m_expirable_accumulated += amount; }

    /**
     * Determines if there is likelihood of needed garbage collection
     *
     * @param cached_data accumulated cell cache data used in TTL garbage estimate
     * @param now current seconds since epoch (avoids call to time())
     */
    bool check_needed(int64_t cached_data, time_t now=0);

    /**
     * Determines if there is likelihood of needed garbage collection
     * given additional deletes and additional accumulated data
     *
     * @param additional_deletes additional deletes to use in calculation
     * @param additional_data additional data to use in calculation
     * @param cached_data accumulated cell cache data used in TTL garbage estimate
     * @param now current seconds since epoch (avoids call to time())
     */
    bool check_needed(uint32_t additional_deletes, int64_t additional_data, int64_t cached_data, time_t now=0);

    /**
     * Sets the garbage statistics measured from a merge scan over
     * all of the CellStores and the CellCache.  This information
     * is used to determine if garbage collection (i.e. major
     * compaction) is necessary and to update the data_target.
     * The data_target represents the amount of data to accumulate
     * before doing garbage collection.  <i>total</i> minus <i>valid</i>
     * is equal to the amount of garbage that has accumulated.
     * 
     * @param total total amount of data read from the merge scan sources
     * @param valid amount of data that was returned by the merge scan
     * @param now current time (seconds since epoch)
     */
    void set_garbage_stats(int64_t total, int64_t valid, time_t now=0);

    bool need_collection() { return m_need_collection; }

    int64_t current_target() { return m_data_target; }
    
  private:

    SchemaPtr m_schema;
    int32_t m_elapsed_target;
    int32_t m_last_clear_time;
    int32_t m_minimum_elapsed_target;
    int32_t m_delete_count;
    int64_t m_expirable_accumulated;
    int64_t m_data_accumulated;
    int64_t m_data_target;
    int64_t m_minimum_data_target;
    int64_t m_min_ttl;
    int64_t m_max_ttl;
    bool m_have_max_versions;
    bool m_need_collection;
  };

} // namespace Hypertable

#endif // HYPERTABLE_ACCESSGROUPGARBAGETRACKER_H
