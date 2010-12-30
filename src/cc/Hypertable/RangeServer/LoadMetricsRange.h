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
#include "Common/Mutex.h"
#include "Common/String.h"

#include "Hypertable/Lib/TableMutator.h"

namespace Hypertable {

  class LoadMetricsRange {
  public:
    LoadMetricsRange(const String &table_id, const String &start_row, const String &end_row);

    void change_rows(const String &start_row, const String &end_row) {
      ScopedLock lock(m_mutex);
      m_new_start_row = start_row;
      m_new_end_row = end_row;
      m_new_rows = true;
    }

    void compute_and_store(TableMutator *mutator, time_t now,
                           uint64_t disk_used, uint64_t memory_used,
                           uint64_t scans, uint64_t updates,
                           uint64_t cells_read, uint64_t cells_written,
                           uint64_t bytes_read, uint64_t bytes_written);

  private:

    void initialize(const String &table_id, const String &start_row, const String &end_row);

    Mutex m_mutex;
    DynamicBuffer m_buffer;
    const char *m_table_id;
    const char *m_start_row;
    const char *m_end_row;
    String m_new_start_row;
    String m_new_end_row;
    bool m_new_rows;
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
