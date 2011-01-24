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

#include "Common/Compat.h"
#include "AsyncComm/Protocol.h"

#include "Common/Error.h"
#include "Common/Logger.h"

#include "TableScannerDispatchHandler.h"
#include "TableScannerHandler.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
TableScannerDispatchHandler::TableScannerDispatchHandler(
  ApplicationQueuePtr &app_queue, TableScannerAsync *scanner, int interval_scanner)
  : m_app_queue(app_queue), m_scanner(scanner), m_interval_scanner(interval_scanner) {
}



/**
 *
 */
void TableScannerDispatchHandler::handle(EventPtr &event) {
  // Create TableScannerHandler and add task to application queue
  m_app_queue->add(new TableScannerHandler(m_scanner, m_interval_scanner, event));
}

