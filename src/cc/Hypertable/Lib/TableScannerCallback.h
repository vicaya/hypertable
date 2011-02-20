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

#ifndef HYPERTABLE_TABLESCANNERCALLBACK_H
#define HYPERTABLE_TABLESCANNERCALLBACK_H

#include "ResultCallback.h"

namespace Hypertable {

  class TableScannerSync;

  /** Represents an open table.
   */
  class TableScannerCallback: public ResultCallback {

  public:

    TableScannerCallback(TableScannerSync *scanner) : m_scanner(scanner) {};

    /**
     * Callback method for successful scan
     *
     * @param scanner
     * @param cells returned cells
     */
    void scan_ok(TableScannerAsync *scanner, ScanCellsPtr &cells);

    /**
     * Callback method for scan errors
     *
     * @param scanner
     * @param error
     * @param error_msg
     * @param eos end of scan
     */
    void scan_error(TableScannerAsync *scanner, int error, const String &error_msg,
                    bool eos);

    /**
     * Mutator callbacks do nothing
     */
    void update_ok(TableMutator *mutator, FailedMutations &failed_mutations) {}
    void update_error(TableMutator *mutator, int error, const String &error_msg) {}


  private:
    TableScannerSync *m_scanner;
  };
  typedef intrusive_ptr<TableScannerCallback> TableScannerCallbackPtr;
} // namesapce Hypertable

#endif // HYPERTABLE_TABLESCANNERCALLBACK_H
