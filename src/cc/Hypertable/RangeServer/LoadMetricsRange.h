/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
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

#ifndef HYPERTABLE_LOADMETRICSRANGE_H
#define HYPERTABLE_LOADMETRICSRANGE_H

#include "Common/DynamicBuffer.h"
#include "Common/String.h"

#include "Hypertable/Lib/TableMutator.h"

namespace Hypertable {

  class LoadMetricsRange {
  public:
    LoadMetricsRange(const String &table_id, const String &end_row);

    void compute_and_store(TableMutator *mutator, time_t now,
                           uint64_t disk_used, uint64_t memory_used,
                           uint64_t scans, uint64_t updates,
                           uint64_t cells_read, uint64_t cells_written,
                           uint64_t bytes_read, uint64_t bytes_written);

  private:
    DynamicBuffer m_buffer;
    const char *m_table_id;
    const char *m_end_row;
    time_t m_timestamp;
    uint64_t m_scans;
    uint64_t m_updates;
    uint64_t m_cells_read;
    uint64_t m_cells_written;
    uint64_t m_bytes_read;
    uint64_t m_bytes_written;
  };
}


#endif // HYPERTABLE_LOADMETRICSRANGE_H
