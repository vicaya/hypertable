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

#ifndef HYPERTABLE_CELLSTORESCANNERINTERVAL_H
#define HYPERTABLE_CELLSTORESCANNERINTERVAL_H

#include "Common/ByteString.h"
#include "Hypertable/Lib/Key.h"

namespace Hypertable {

  class CellStoreScannerInterval {
  public:
    virtual void forward() = 0;
    virtual bool get(Key &key, ByteString &value) = 0;
    virtual ~CellStoreScannerInterval() { }
  protected:
    struct BlockInfo {
      int64_t offset;
      int64_t zlength;
      const uint8_t *base;
      const uint8_t *end;
    };

  };

}

#endif // HYPERTABLE_CELLSTORESCANNERINTERVAL_H
