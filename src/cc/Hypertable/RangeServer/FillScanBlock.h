/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_FILLSCANBLOCK_H
#define HYPERTABLE_FILLSCANBLOCK_H

#include "Common/DynamicBuffer.h"

#include "CellListScanner.h"

namespace Hypertable {

  bool FillScanBlock(CellListScannerPtr &scanner, DynamicBuffer &dbuf,
                     int64_t buffer_size);

}

#endif // HYPERTABLE_FILLSCANBLOCK_H

