/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_MAINTENANCEPRIORITIZERLOWMEMORY_H
#define HYPERTABLE_MAINTENANCEPRIORITIZERLOWMEMORY_H

#include "MaintenancePrioritizer.h"

namespace Hypertable {

  class MaintenancePrioritizerLowMemory : public MaintenancePrioritizer {
  public:
    MaintenancePrioritizerLowMemory(RSStatsPtr &server_stats)
      : MaintenancePrioritizer(server_stats) { }
    virtual void prioritize(RangeStatsVector &range_data,
                            MemoryState &memory_state, String &trace_str);

  private:

    void assign_priorities_all(RangeStatsVector &range_data, CommitLog *log,
                               int64_t prune_threshold, MemoryState &memory_state,
                               int32_t &priority, String &trace_str);

    void assign_priorities_user(RangeStatsVector &range_data,
                                MemoryState &memory_state,
                                int32_t &priority, String &trace_str);

  };

}

#endif // HYPERTABLE_MAINTENANCEPRIORITIZERLOWMEMORY_H


