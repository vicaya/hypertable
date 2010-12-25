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

#ifndef HYPERTABLE_RANGESTATSGATHERER_H
#define HYPERTABLE_RANGESTATSGATHERER_H

#include "Common/PageArena.h"
#include "Common/ReferenceCount.h"

#include "Hypertable/Lib/TableMutator.h"

#include "MaintenanceQueue.h"
#include "Range.h"
#include "TableInfoMap.h"

namespace Hypertable {

  typedef std::vector<Range::MaintenanceData *> RangeStatsVector;

  class RangeStatsGatherer : public ReferenceCount {
  public:
    RangeStatsGatherer(TableInfoMapPtr &table_info_map) : m_table_info_map(table_info_map) { }

    virtual ~RangeStatsGatherer() { }

    void fetch(RangeStatsVector &range_stats, size_t *lenp=0, TableMutator *mutator=0);

    void clear();

  private:
    ByteArena m_arena;
    TableInfoMapPtr  m_table_info_map;
    std::vector<RangePtr> m_range_vec;

  };
  typedef intrusive_ptr<RangeStatsGatherer> RangeStatsGathererPtr;

}

#endif // HYPERTABLE_RANGESTATSGATHERER_H


