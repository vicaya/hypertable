/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
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

#ifndef HYPERTABLE_STATSTABLE_H
#define HYPERTABLE_STATSTABLE_H

#include "Common/StatsSerializable.h"

namespace Hypertable {

  class StatsTable : public StatsSerializable {
  public:
    StatsTable();
    StatsTable(const Hypertable::StatsTable &other) : StatsSerializable(TABLE, 1) {
      id = other.id;
      group_count = other.group_count;
      memcpy(group_ids, other.group_ids, group_count);
      table_id = other.table_id;
      range_count = other.range_count;
      cell_count = other.cell_count;
      file_count = other.file_count;
      scans = other.scans;
      cells_scanned = other.cells_returned;
      cells_returned = other.cells_returned;
      bytes_scanned = other.bytes_scanned;
      bytes_returned = other.bytes_returned;
      updates = other.updates;
      cells_written = other.cells_written;
      bytes_written = other.bytes_written;
      disk_used = other.disk_used;
      key_bytes = other.key_bytes;
      value_bytes = other.value_bytes;
      compression_ratio = other.compression_ratio;
      memory_used = other.memory_used;
      memory_allocated = other.memory_allocated;
      shadow_cache_memory = other.shadow_cache_memory;
      block_index_memory = other.block_index_memory;
      bloom_filter_memory = other.bloom_filter_memory;
      bloom_filter_accesses = other.bloom_filter_accesses;
      bloom_filter_maybes = other.bloom_filter_maybes;
    }
    void clear();
    bool operator==(const StatsTable &other) const;
    bool operator!=(const StatsTable &other) const {
      return !(*this == other);
    }

    String table_id;
    uint32_t range_count;
    uint64_t cell_count;
    uint64_t file_count;
    uint64_t scans;
    uint64_t cells_scanned;
    uint64_t cells_returned;
    uint64_t bytes_scanned;
    uint64_t bytes_returned;
    uint64_t updates;
    uint64_t cells_written;
    uint64_t bytes_written;
    uint64_t disk_used;
    uint64_t key_bytes;
    uint64_t value_bytes;
    double compression_ratio;
    uint64_t memory_used;
    uint64_t memory_allocated;
    uint64_t shadow_cache_memory;
    uint64_t block_index_memory;
    uint64_t bloom_filter_memory;
    uint64_t bloom_filter_accesses;
    uint64_t bloom_filter_maybes;

  protected:
    virtual size_t encoded_length_group(int group) const;
    virtual void encode_group(int group, uint8_t **bufp) const;
    virtual void decode_group(int group, uint16_t len, const uint8_t **bufp, size_t *remainp);
  };
}


#endif // HYPERTABLE_STATSTABLE_H


