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
    virtual size_t size() { return 56; }
    virtual void serialize(uint8_t *buf);
    virtual void deserialize(const uint8_t *buf);
    virtual void display(std::ostream &os);

    uint32_t  fix_index_offset;
    uint32_t  var_index_offset;
    uint32_t  filter_offset;
    uint32_t  index_entries;
    uint32_t  total_entries;
    uint32_t  num_filter_items;
    union {
      float    filter_false_positive_rate;
      uint32_t filter_false_positive_rate_i32;
    };
    uint32_t  blocksize;
    int64_t   revision;
    uint32_t  table_id;
    uint32_t  table_generation;
    union {
      float compression_ratio;
      uint32_t compression_ratio_i32;
    };
    uint16_t  compression_type;
    uint16_t  version;
    
    boost::any get(const String& prop) {
      if     (prop == "version")                return version;
      else if (prop == "fix_index_offset")      return fix_index_offset;
      else if (prop == "var_index_offset")      return var_index_offset;
      else if (prop == "filter_offset")         return filter_offset;
      else if (prop == "index_entries")         return index_entries;
      else if (prop == "total_entries")         return total_entries;
      else if (prop == "num_filter_items")      return num_filter_items;
      else if (prop == "filter_false_positive_rate") 
          return filter_false_positive_rate;
      else if (prop == "blocksize")             return blocksize;
      else if (prop == "revision")              return revision;
      else if (prop == "table_id")              return table_id;
      else if (prop == "table_generation")      return table_generation;
      else if (prop == "compression_ratio")     return compression_ratio;
      else if (prop == "compression_type")      return compression_type;
      else                                      return boost::any();
    }
    
  };

}

#endif // HYPERTABLE_CELLSTORETRAILERV0_H
