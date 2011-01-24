/**
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_RESULT_H
#define HYPERTABLE_RESULT_H

#include "Common/ReferenceCount.h"
#include "ScanCells.h"

namespace Hypertable {

  class TableMutator;
  class TableScannerAsync;
  typedef std::pair<Cell, int> FailedMutation;
  typedef std::vector<FailedMutation> FailedMutations;

  class Result: public ReferenceCount {
    public:

      Result(TableScannerAsync *scanner, ScanCellsPtr &cells);
      Result(TableScannerAsync *scanner, int error,
             const String &error_msg);
      Result(TableMutator *, FailedMutations &failed_mutations);
      Result(TableMutator *, int error, const String &error_msg);


      bool is_error() const { return m_iserror; }
      bool is_scan() const { return m_isscan; }
      bool is_update() const { return !m_isscan; }
      TableScannerAsync *get_scanner();
      TableMutator *get_mutator();
      void get_cells(Cells &cells);
      void get_error(int &error, String &m_error_msg);

    private:
      TableScannerAsync *m_scanner;
      TableMutator *m_mutator;
      ScanCellsPtr m_cells;
      int m_error;
      String m_error_msg;
      bool m_isscan;
      bool m_iserror;

  };
  typedef intrusive_ptr<Result> ResultPtr;
}

#endif // HYPERTABLE_FUTURE_H

