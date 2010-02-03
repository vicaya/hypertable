/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#ifndef HYPERTABLE_TABLESPLIT_H
#define HYPERTABLE_TABLESPLIT_H

#include "Common/PageArena.h"

#include <vector>

namespace Hypertable {

/**
 * Represents a table split
 */
struct TableSplit {
  TableSplit() : start_row(0), end_row(0), location(0) { }

  void clear() {
    start_row = 0;
    end_row = 0;
    location = 0;
  }

  const char *start_row;
  const char *end_row;
  const char *location;
};

std::ostream &operator<<(std::ostream &os, const TableSplit &ts);

// Builds a list of table splits (could manage storage)
class TableSplitBuilder {
public:

  TableSplitBuilder(CharArena &a) : m_arena(a) { }

  void set_start_row(const String &row) { 
    m_table_split.start_row = m_arena.dup(row.c_str());
  }

  void set_end_row(const String &row) { 
    m_table_split.end_row = m_arena.dup(row.c_str());
  }

  void set_location(const String &loc) { 
    m_table_split.location = m_arena.dup(loc.c_str());
  }

  void clear() { m_table_split.clear(); }

  TableSplit &get() { return m_table_split; }

  CharArena &arena() { return m_arena; }

private:
  CharArena &m_arena;
  TableSplit m_table_split;
};

class TableSplitsContainer : public std::vector<TableSplit> {
public:
  CharArena &arena() { return m_arena; }
private:  
  CharArena m_arena;  
};



} // namespace Hypertable

#endif // HYPERTABLE_TABLESPLIT_H
