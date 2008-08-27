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

#ifndef HYPERTABLE_CELLSTORETRAILERV0_H
#define HYPERTABLE_CELLSTORETRAILERV0_H


#include "CellStoreTrailer.h"

namespace Hypertable {

  class CellStoreTrailerV0 : public CellStoreTrailer {
  public:
    CellStoreTrailerV0();
    virtual ~CellStoreTrailerV0() { return; }
    virtual void clear();
    virtual size_t size() { return 40; }
    virtual void serialize(uint8_t *buf);
    virtual void deserialize(const uint8_t *buf);
    virtual void display(std::ostream &os);

    uint32_t  fix_index_offset;
    uint32_t  var_index_offset;
    uint32_t  filter_offset;
    uint32_t  index_entries;
    uint32_t  total_entries;
    uint32_t  blocksize;
    int64_t   revision;
    union {
      float compression_ratio;
      uint32_t compression_ratio_i32;
    };
    uint16_t  compression_type;
    uint16_t  version;
  };

}

#endif // HYPERTABLE_CELLSTORETRAILERV0_H
