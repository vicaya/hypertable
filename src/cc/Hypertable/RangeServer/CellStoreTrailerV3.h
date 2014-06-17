/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#ifndef HYPERTABLE_CELLSTORETRAILERV3_H
#define HYPERTABLE_CELLSTORETRAILERV3_H

#include <boost/any.hpp>

#include "CellStoreTrailer.h"

namespace Hypertable {

  class CellStoreTrailerV3 : public CellStoreTrailer {
  public:
    CellStoreTrailerV3();
    virtual ~CellStoreTrailerV3() { return; }
    virtual void clear();
    virtual size_t size() { return 144; }
    virtual void serialize(uint8_t *buf);
    virtual void deserialize(const uint8_t *buf);
    virtual void display(std::ostream &os);
    virtual void display_multiline(std::ostream &os);

    int64_t fix_index_offset;
    int64_t var_index_offset;
    int64_t filter_offset;
    int64_t index_entries;
    int64_t total_entries;
    int64_t filter_length;
    int64_t filter_items_estimate;
    int64_t filter_items_actual;
    int64_t  blocksize;
    int64_t  revision;
    int64_t  timestamp_min;
    int64_t  timestamp_max;
    int64_t  expiration_time;
    int64_t  create_time;
    int64_t  expirable_data;
    uint32_t table_id;
    uint32_t table_generation;
    uint32_t flags;
    union {
      float compression_ratio;
      uint32_t compression_ratio_i32;
    };
    uint16_t  compression_type;
    uint16_t  key_compression_scheme;
    uint8_t   bloom_filter_mode;
    uint8_t   bloom_filter_hash_count;
    uint16_t  version;

    enum Flags { INDEX_64BIT = 0x00000001 };

    boost::any get(const String& prop) {
      if     (prop == "version")                return version;
      else if (prop == "fix_index_offset")      return fix_index_offset;
      else if (prop == "var_index_offset")      return var_index_offset;
      else if (prop == "filter_offset")         return filter_offset;
      else if (prop == "index_entries")         return index_entries;
      else if (prop == "total_entries")         return total_entries;
      else if (prop == "filter_length")         return filter_length;
      else if (prop == "filter_items_estimate") return filter_items_estimate;
      else if (prop == "filter_items_actual")   return filter_items_actual;
      else if (prop == "blocksize")             return blocksize;
      else if (prop == "revision")              return revision;
      else if (prop == "timestamp_min")         return timestamp_min;
      else if (prop == "timestamp_max")         return timestamp_max;
      else if (prop == "expiration_time")       return expiration_time;
      else if (prop == "create_time")           return create_time;
      else if (prop == "expirable_data")        return expirable_data;
      else if (prop == "table_id")              return table_id;
      else if (prop == "table_generation")      return table_generation;
      else if (prop == "flags")                 return flags;
      else if (prop == "compression_ratio")     return compression_ratio;
      else if (prop == "compression_type")      return compression_type;
      else if (prop == "bloom_filter_mode")     return bloom_filter_mode;
      else if (prop == "bloom_filter_hash_count") return bloom_filter_hash_count;
      else                                      return boost::any();
    }

  };

}

#endif // HYPERTABLE_CELLSTORETRAILERV3_H
