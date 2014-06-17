/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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

#ifndef HYPERTABLE_CELLSTORETRAILERV5_H
#define HYPERTABLE_CELLSTORETRAILERV5_H

#include <boost/any.hpp>

#include "CellStoreTrailer.h"

namespace Hypertable {

  class CellStoreTrailerV5 : public CellStoreTrailer {
  public:
    CellStoreTrailerV5();
    virtual ~CellStoreTrailerV5() { return; }
    virtual void clear();
    virtual size_t size() { return 192; }
    virtual void serialize(uint8_t *buf);
    virtual void deserialize(const uint8_t *buf);
    virtual void display(std::ostream &os);
    virtual void display_multiline(std::ostream &os);

    int64_t fix_index_offset;
    int64_t var_index_offset;
    int64_t filter_offset;
    int64_t replaced_files_offset;
    int64_t index_entries;
    int64_t total_entries;
    int64_t filter_length;
    int64_t filter_items_estimate;
    int64_t filter_items_actual;
    int64_t replaced_files_length;
    uint32_t replaced_files_entries;
    int64_t blocksize;
    int64_t revision;
    int64_t timestamp_min;
    int64_t timestamp_max;
    int64_t expiration_time;
    int64_t create_time;
    int64_t expirable_data;
    int64_t delete_count;
    int64_t key_bytes;
    int64_t value_bytes;
    uint32_t table_id;
    uint32_t table_generation;
    uint32_t flags;
    uint32_t alignment;
    union {
      float compression_ratio;
      uint32_t compression_ratio_i32;
    };
    uint16_t  compression_type;
    uint16_t  key_compression_scheme;
    uint8_t   bloom_filter_mode;
    uint8_t   bloom_filter_hash_count;
    uint16_t  version;

    enum Flags { INDEX_64BIT = 1,
                 MAJOR_COMPACTION = 2,
                 SPLIT = 4
    };

    boost::any get(const String& prop) {
      if     (prop == "version")                return version;
      else if (prop == "fix_index_offset")      return fix_index_offset;
      else if (prop == "var_index_offset")      return var_index_offset;
      else if (prop == "filter_offset")         return filter_offset;
      else if (prop == "replaced_files_offset") return replaced_files_offset;
      else if (prop == "index_entries")         return index_entries;
      else if (prop == "total_entries")         return total_entries;
      else if (prop == "filter_length")         return filter_length;
      else if (prop == "filter_items_estimate") return filter_items_estimate;
      else if (prop == "filter_items_actual")   return filter_items_actual;
      else if (prop == "replaced_files_length") return replaced_files_length;
      else if (prop == "replaced_files_entries") return replaced_files_entries;
      else if (prop == "blocksize")             return blocksize;
      else if (prop == "revision")              return revision;
      else if (prop == "timestamp_min")         return timestamp_min;
      else if (prop == "timestamp_max")         return timestamp_max;
      else if (prop == "expiration_time")       return expiration_time;
      else if (prop == "create_time")           return create_time;
      else if (prop == "expirable_data")        return expirable_data;
      else if (prop == "delete_count")          return delete_count;
      else if (prop == "key_bytes")             return key_bytes;
      else if (prop == "value_bytes")           return value_bytes;
      else if (prop == "table_id")              return table_id;
      else if (prop == "table_generation")      return table_generation;
      else if (prop == "flags")                 return flags;
      else if (prop == "alignment")             return alignment;
      else if (prop == "compression_ratio")     return compression_ratio;
      else if (prop == "compression_type")      return compression_type;
      else if (prop == "bloom_filter_mode")     return bloom_filter_mode;
      else if (prop == "bloom_filter_hash_count") return bloom_filter_hash_count;
      else                                      return boost::any();
    }

  };

}

#endif // HYPERTABLE_CELLSTORETRAILERV5_H
