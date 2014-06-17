/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_CELLS_H
#define HYPERTABLE_CELLS_H

#include <vector>

#include "Common/ReferenceCount.h"
#include "Common/PageArenaAllocator.h"
#include "Cell.h"

namespace Hypertable {

typedef PageArenaAllocator<Cell> CellAlloc;
typedef std::vector<Cell, CellAlloc> Cells;

class CellsBuilder : public ReferenceCount {
public:
  CellsBuilder(size_t size_hint = 128)
    : m_cells(CellAlloc(m_arena)), m_size_hint(size_hint) {}

  size_t size() const {
    return m_cells.size();
  }

  void get_cell(Cell &cc, size_t ii) {
    cc = m_cells[ii];
  }

  void add(const Cell &cell, bool own = true) {

    if (!m_cells.capacity())
      m_cells.reserve(m_size_hint);

    if (!own) {
      m_cells.push_back(cell);
      return;
    }

    Cell copy;

    if (cell.row_key)
      copy.row_key = m_arena.dup(cell.row_key);

    if (cell.column_family)
      copy.column_family = m_arena.dup(cell.column_family);

    if (cell.column_qualifier)
      copy.column_qualifier = m_arena.dup(cell.column_qualifier);

    copy.timestamp = cell.timestamp;
    copy.revision = cell.revision;

    if (cell.value) {
      copy.value = (uint8_t *)m_arena.dup(cell.value, cell.value_len);
      copy.value_len = cell.value_len;
    }
    copy.flag = cell.flag;
    m_cells.push_back(copy);
  }

  void get(Cells &cells) { cells = m_cells; }
  Cells &get() { return m_cells; }
  const Cells &get() const { return m_cells; }

  void swap(CellsBuilder &x) {
    m_cells.swap(x.m_cells);
    m_arena.swap(x.m_arena);
  }

  void clear() { m_cells.clear(); }

protected:
  CharArena m_arena;
  Cells m_cells;
  size_t m_size_hint;
};

typedef intrusive_ptr<CellsBuilder> CellsBuilderPtr;
} // namespace Hypertable

#endif // HYPERTABLE_CELLS_H
