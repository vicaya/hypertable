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

#include "Common/Compat.h"

#include <ctime>

#include "RangeStatsGatherer.h"

using namespace Hypertable;

void RangeStatsGatherer::fetch(RangeStatsVector &range_stats, 
                               size_t *lenp, TableMutator *mutator) {
  std::vector<TableInfoPtr> table_vec;

  range_stats.clear();

  clear();

  m_table_info_map->get_all(table_vec);

  m_range_vec.reserve( m_table_info_map->get_range_count() );

  if (table_vec.empty())
    return;

  time_t now = time(0);

  if (lenp)
    *lenp = table_vec.size();

  for (size_t i=0,j=0; i<table_vec.size(); i++) {
    table_vec[i]->get_range_vector(m_range_vec);
    for (; j<m_range_vec.size(); j++)
      range_stats.push_back(m_range_vec[j]->get_maintenance_data(m_arena, now, mutator));
  }

}


void RangeStatsGatherer::clear() {
  m_arena.free();
  m_range_vec.clear();
}
