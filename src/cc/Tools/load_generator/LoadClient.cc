/**
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
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
#include "LoadClient.h"

#ifdef HT_WITH_THRIFT
#include "ThriftBroker/Client.h"
#include "ThriftBroker/Config.h"
#endif

LoadClient::LoadClient(const String &config_file, bool thrift): m_thrift(thrift),
    m_native_client(0), m_ns(0), m_native_table(0), m_native_table_open(false),
    m_native_mutator(0), m_native_scanner(0)
{
#ifdef HT_WITH_THRIFT
  m_thrift_namespace = 0;
  m_thrift_mutator = 0;
  m_thrift_scanner = 0;
#endif

  if(m_thrift) {
#ifdef HT_WITH_THRIFT
    m_thrift_client.reset(new Thrift::Client("localhost", 38080));
#else
    HT_FATAL("Thrift support not installed");
#endif
  }
  else {
    m_native_client = new Hypertable::Client(config_file);
    m_ns = m_native_client->open_namespace("/");
  }
}

LoadClient::LoadClient(bool thrift):m_thrift(thrift),
    m_native_client(0), m_ns(0), m_native_table(0), m_native_table_open(false),
    m_native_mutator(0), m_native_scanner(0)
{
#ifdef HT_WITH_THRIFT
  m_thrift_namespace = 0;
  m_thrift_mutator = 0;
  m_thrift_scanner = 0;
#endif

  if(m_thrift) {
#ifdef HT_WITH_THRIFT
    m_thrift_client.reset(new Thrift::Client("localhost", 38080));
    m_thrift_namespace = m_thrift_client->open_namespace("/");
#else
    HT_FATAL("Thrift support not installed");
#endif
  }
  else {
    m_native_client = new Hypertable::Client();
    m_ns = m_native_client->open_namespace("/");
  }
}

void
LoadClient::create_mutator(const String &tablename, int mutator_flags)
{
  if(m_thrift) {
#ifdef HT_WITH_THRIFT
    m_thrift_mutator = m_thrift_client->open_mutator(m_thrift_namespace, tablename,
                                                     mutator_flags, 0);
#endif
  }
  else {
    if (!m_native_table_open) {
      m_native_table = m_ns->open_table(tablename);
      m_native_table_open = true;
    }
    m_native_mutator = m_native_table->create_mutator(0, mutator_flags);
  }
}

void
LoadClient::set_cells(const Cells &cells)
{
  if (m_thrift) {
#ifdef HT_WITH_THRIFT
    vector<ThriftGen::Cell> thrift_cells;
    foreach(const Cell &cell , cells) {
      thrift_cells.push_back(ThriftGen::make_cell((const char*)cell.row_key,
          (const char*)cell.column_family,(const char*)cell.column_qualifier,
          string((const char*)cell.value, cell.value_len), cell.timestamp, cell.revision,
          (ThriftGen::CellFlag) cell.flag));
    }
    m_thrift_client->set_cells(m_thrift_mutator, thrift_cells);
#endif
  }
  else {
    m_native_mutator->set_cells(cells);
  }
}


void
LoadClient::set_delete(const KeySpec &key) {
  if (m_thrift) {
#ifdef HT_WITH_THRIFT
    vector<ThriftGen::Cell> thrift_cells;
    ThriftGen::CellFlag flag = ThriftGen::INSERT;

    if (key.column_family == 0 || *key.column_family == '\0')
      flag = ThriftGen::DELETE_ROW;
    if (key.column_qualifier == 0 || *key.column_qualifier == '\0')
      flag = ThriftGen::DELETE_CF;
    else
      flag = ThriftGen::DELETE_CELL;

    thrift_cells.push_back(ThriftGen::make_cell((const char *)key.row,
        key.column_family, key.column_qualifier, std::string(""),
        key.timestamp, key.revision, flag));

    m_thrift_client->set_cells(m_thrift_mutator, thrift_cells);
#endif
  }
  else {
    m_native_mutator->set_delete(key);
  }
}

void
LoadClient::flush()
{
  if (m_thrift) {
#ifdef HT_WITH_THRIFT
    m_thrift_client->flush_mutator(m_thrift_mutator);
#endif
  }
  else {
    m_native_mutator->flush();
  }

}

void
LoadClient::create_scanner(const String &tablename, const ScanSpec &scan_spec)
{
  if (m_thrift) {
#ifdef HT_WITH_THRIFT
    //copy scanspec column and first row interval
    ThriftGen::ScanSpec thrift_scan_spec;
    ThriftGen::RowInterval thrift_row_interval;
    thrift_row_interval.start_row = scan_spec.row_intervals[0].start;
    thrift_row_interval.end_row = scan_spec.row_intervals[0].end;
    thrift_row_interval.start_inclusive = scan_spec.row_intervals[0].start_inclusive;
    thrift_row_interval.end_inclusive = scan_spec.row_intervals[0].end_inclusive;
    thrift_row_interval.__isset.start_row = thrift_row_interval.__isset.end_row = true;
    thrift_row_interval.__isset.start_inclusive = thrift_row_interval.__isset.end_inclusive = true;

    thrift_scan_spec.columns.push_back(scan_spec.columns[0]);
    thrift_scan_spec.row_intervals.push_back(thrift_row_interval);
    thrift_scan_spec.__isset.columns = thrift_scan_spec.__isset.row_intervals = true;

    m_thrift_scanner = m_thrift_client->open_scanner(m_thrift_namespace, tablename,
                                                     thrift_scan_spec, true);
#endif
  }
  else {
    if (!m_native_table_open) {
      m_native_table = m_ns->open_table(tablename);
      m_native_table_open = true;
    }
    m_native_scanner = m_native_table->create_scanner(scan_spec);
  }

}

uint64_t
LoadClient::get_all_cells()
{
  if(m_thrift) {
    uint64_t bytes_scanned=0;
#ifdef HT_WITH_THRIFT
    vector<ThriftGen::Cell> cells;

    do {
      m_thrift_client->next_cells(cells, m_thrift_scanner);
      foreach(const ThriftGen::Cell &cell, cells) {
        bytes_scanned += cell.key.row.size() + cell.key.column_family.size() +
                         cell.key.column_qualifier.size() + 8 + 8 + 2 + cell.value.size();
      }
    } while (cells.size());
#endif
    return bytes_scanned;
  }
  else {
    Cell cell;
    while (m_native_scanner->next(cell))
      ;
    return m_native_scanner->bytes_scanned();
  }

}

void
LoadClient::close_scanner()
{
  if (m_thrift) {
#ifdef HT_WITH_THRIFT
    m_thrift_client->close_scanner(m_thrift_scanner);
#endif
  }
  else {
    m_native_scanner = 0;
  }
}

LoadClient::~LoadClient()
{
  if (m_thrift) {
#ifdef HT_WITH_THRIFT
    m_thrift_client->close_namespace(m_thrift_namespace);
#endif
  }
}
