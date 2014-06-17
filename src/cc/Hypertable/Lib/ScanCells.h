/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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

#ifndef HYPERTABLE_SCANCELLS_H
#define HYPERTABLE_SCANCELLS_H

#include "Common/Compat.h"

#include <vector>

#include "Common/StringExt.h"
#include "Common/ReferenceCount.h"

#include "Cells.h"
#include "ScanBlock.h"
#include "Schema.h"

namespace Hypertable {

using namespace std;
class IntervalScannerAsync;

/**
 * This class takes allows vector access to a set of cells contained in an EventPtr without
 * any copying.
 */
class ScanCells : public ReferenceCount {

public:
  ScanCells() : m_eos(false){}

  void get(Cells &cells) { m_cells->get(cells); }
  void get_cell_unchecked(Cell &cc, size_t ii) { m_cells->get_cell(cc, ii); }
  void set_eos() { m_eos = true; }
  bool get_eos() const { return m_eos; }
  size_t size() const {
    if (m_cells)
      return m_cells->size();
    else
      return 0;
  }

protected:

  friend class IntervalScannerAsync;

  /**
   * @param event the event that contains the scan results
   * @param scanner_id scanner_id for the scanner
   * @return true if this if this event has the eos bit set
   */
  bool add(EventPtr &event, int *scanner_id);


  /**
   * @param schema is the schema for the table being scanned
   * @param end_row the end_row of the scan for which we got these results
   * @param end_inclusive is the end_row included in the scan
   * @param row_limit row limit specified for scan
   * @param rows_seen number of unique rows seen
   * @param cur_row current/last row seen by scanner
   * @param eos true if these results have the eos bit set
   * @param bytes_scanned number of bytes read
   * @return true if scan has reached end
   */
  bool load(SchemaPtr &schema,
            const String &end_row, bool end_inclusive, int row_limit,
            int *rows_seen, String &cur_row, CstrSet &rowset, int64_t *bytes_scanned);

  vector<ScanBlockPtr> m_scanblocks;
  ScanBlock m_scanblock;
  CellsBuilderPtr m_cells;
  bool m_eos;
}; // ScanCells

typedef intrusive_ptr<ScanCells> ScanCellsPtr;

} // namespace Hypertable

#endif // HYPERTABLE_SCANCELLS_H
