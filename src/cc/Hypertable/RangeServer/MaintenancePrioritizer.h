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

#ifndef HYPERTABLE_MAINTENANCEPRIORITIZER_H
#define HYPERTABLE_MAINTENANCEPRIORITIZER_H

#include <boost/thread/xtime.hpp>

#include "Common/Logger.h"
#include "Common/ReferenceCount.h"

#include "Global.h"
#include "RangeServerStats.h"
#include "RangeStatsGatherer.h"

namespace Hypertable {

  class MaintenancePrioritizer : public ReferenceCount {
  public:

    class MemoryState {
    public:
      MemoryState() : limit(Global::memory_limit), balance(0), needed(0) { }
      void decrement_needed(int64_t amount) {
        if (amount > needed)
          needed = 0;
        else
          needed -= amount;
      }
      bool need_more() { return needed > 0; }
      int64_t limit;
      int64_t balance;
      int64_t needed;
    };

    MaintenancePrioritizer(RangeServerStatsPtr &server_stats) 
      : m_cellstore_minimum_size(0), m_server_stats(server_stats) { }

    virtual void prioritize(RangeStatsVector &range_data,
                            MemoryState &memory_state, String &trace_str) = 0;

  protected:

    int64_t m_cellstore_minimum_size;
    RangeServerStatsPtr m_server_stats;

    bool schedule_inprogress_splits(RangeStatsVector &range_data,
                                    MemoryState &memory_state,
                                    int32_t &priority, String &trace_str);

    bool schedule_splits(RangeStatsVector &range_data,
                         MemoryState &memory_state,
                         int32_t &priority, String &trace_str);

    bool schedule_necessary_compactions(RangeStatsVector &range_data,
                            CommitLog *log, int64_t prune_threshold,
                            MemoryState &memory_state,
                            int32_t &priority, String &trace_str);

    bool purge_shadow_caches(RangeStatsVector &range_data,
                             MemoryState &memory_state,
                             int32_t &priority, String &trace_str);

    bool purge_cellstore_indexes(RangeStatsVector &range_data,
                                 MemoryState &memory_state,
                                 int32_t &priority, String &trace_str);

    bool compact_cellcaches(RangeStatsVector &range_data,
                            MemoryState &memory_state,
                            int32_t &priority, String &trace_str);


  };
  typedef intrusive_ptr<MaintenancePrioritizer> MaintenancePrioritizerPtr;

}

#endif // HYPERTABLE_MAINTENANCEPRIORITIZER_H


