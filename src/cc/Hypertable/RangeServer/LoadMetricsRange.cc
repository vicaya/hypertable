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
#include "Common/Compat.h"
#include "Common/String.h"

#include <ctime>

#include "LoadMetricsRange.h"
#include "Location.h"

using namespace Hypertable;

LoadMetricsRange::LoadMetricsRange(const String &table_id, const String &end_row)
  : m_timestamp(time(0)), m_scans(0), m_updates(0),
    m_cells_read(0), m_cells_written(0), m_bytes_read(0), m_bytes_written(0) {

  m_buffer.reserve(table_id.length() + 1 + end_row.length() + 1);

  // save table ID
  m_table_id = (const char *)m_buffer.ptr;
  m_buffer.add(table_id.c_str(), table_id.length()+1);

  // save end row
  m_end_row = (const char *)m_buffer.ptr;
  m_buffer.add(end_row.c_str(), end_row.length()+1);

}


/**
 *  Value format for version 1:
 *
 *  v1:<timestamp>,<disk>,<memory>,<scan-rate>,<update-rate>,<cell-read-rate>,<cell-write-rate>,<byte-read-rate>,<byte-write-rate>
 */

void LoadMetricsRange::compute_and_store(TableMutator *mutator, time_t now,
                                         uint64_t disk_used, uint64_t memory_used,
                                         uint64_t scans, uint64_t updates,
                                         uint64_t cells_read, uint64_t cells_written,
                                         uint64_t bytes_read, uint64_t bytes_written) {

  if ((now - m_timestamp) <= 0)
    return;

  time_t rounded_time = (now+30) - ((now+30)%60);
  double time_interval = (double)(now - m_timestamp);
  double scan_rate = (double)(scans-m_scans) / time_interval;
  double update_rate = (double)(updates-m_updates) / time_interval;
  double cell_read_rate = (double)(cells_read-m_cells_read) / time_interval;
  double cell_write_rate = (double)(cells_written-m_cells_written) / time_interval;
  double byte_read_rate = (double)(bytes_read-m_bytes_read) / time_interval;
  double byte_write_rate = (double)(bytes_written-m_bytes_written) / time_interval;

  String value = format("1:%ld:%llu,%llu,%.6f,%.6f,%.6f,%.6f,%.6f,%.6f",
                        rounded_time, (Llu)disk_used, (Llu)memory_used,
                        scan_rate, update_rate, cell_read_rate, cell_write_rate,
                        byte_read_rate, byte_write_rate);

  KeySpec key;
  String row = Location::get() + ":" + m_table_id;

  key.row = row.c_str();
  key.row_len = row.length();
  key.column_family = "range";
  key.column_qualifier = m_end_row;
  key.column_qualifier_len = strlen(m_end_row);
  
  try {
    mutator->set(key, (uint8_t *)value.c_str(), value.length()+1);
    mutator->flush();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << "Problem updating sys/RS_METRICS - " << e << HT_END;
  }

  m_timestamp = now;
  m_scans = scans;
  m_updates = updates;
  m_cells_read = cells_read;
  m_cells_written = cells_written;
  m_bytes_read = bytes_read;
  m_bytes_written = bytes_written;  
}
