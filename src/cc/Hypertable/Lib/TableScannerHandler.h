/** -*- C++ -*-
 * Copyright (C) 2011  Hypertable, Inc.
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

#ifndef HYPERTABLE_TABLESCANNERHANDLER_H
#define HYPERTABLE_TABLESCANNERHANDLER_H

#include "AsyncComm/ApplicationHandler.h"

namespace Hypertable {

class TableScannerAsync;

struct TableScannerHandler: ApplicationHandler {
  TableScannerHandler(TableScannerAsync *scanner, int interval_scanner, EventPtr &event,
    bool is_create)
    : ApplicationHandler(event), m_scanner(scanner), m_interval_scanner(interval_scanner),
      m_is_create(is_create){ }

  virtual void run();

  TableScannerAsync  *m_scanner;
  int m_interval_scanner;
  bool m_is_create;
};

}// namespace Hypertable
#endif /* HYPERTABLE_TABLESCANNERHANDLER_H */
