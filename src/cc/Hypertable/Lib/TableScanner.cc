/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <vector>

#include "Key.h"
#include "TableScanner.h"

using namespace Hypertable;

/** Constructor  */
TableScanner::TableScanner(ConnectionManagerPtr &conn_manager_ptr, TableIdentifierT *table_ptr, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr, ScanSpecificationT &scan_spec) : m_conn_manager_ptr(conn_manager_ptr), m_schema_ptr(schema_ptr), m_range_locator_ptr(range_locator_ptr), m_range_server(conn_manager_ptr->get_comm(), 30), m_started(false), m_eos(false), m_readahead(true), m_fetch_outstanding(false) {
  char *str;

  m_scan_spec.rowLimit = scan_spec.rowLimit;
  m_scan_spec.cellLimit = scan_spec.cellLimit;

  // deep copy columns vector
  for (size_t i=0; i<scan_spec.columns.size(); i++) {
    str = new char [ strlen(scan_spec.columns[i]) + 1 ];
    strcpy(str, scan_spec.columns[i]);
    m_scan_spec.columns.push_back(str);
  }

  // deep copy start row
  if (scan_spec.startRow) {
    str = new char [ strlen(scan_spec.startRow) + 1 ];
    strcpy(str, scan_spec.startRow);
    m_scan_spec.startRow = str;
  }
  else
    m_scan_spec.startRow = 0;
  m_scan_spec.startRowInclusive = scan_spec.startRowInclusive;

  // deep copy end row
  if (scan_spec.endRow) {
    str = new char [ strlen(scan_spec.endRow) + 1 ];
    strcpy(str, scan_spec.endRow);
    m_scan_spec.endRow = str;
  }
  else
    m_scan_spec.endRow = 0;
  m_scan_spec.endRowInclusive = scan_spec.endRowInclusive;

  if (m_scan_spec.rowLimit == 1 ||
      ((m_scan_spec.startRow && m_scan_spec.endRow) && !strcmp(m_scan_spec.startRow, m_scan_spec.endRow)))
    m_readahead = false;

  memcpy(&m_scan_spec.interval, &scan_spec.interval, sizeof(m_scan_spec.interval));

  // deep copy TableIdentifierT
  {
    char *name = new char [ strlen(table_ptr->name) + 1 ];
    memcpy(&m_table, table_ptr, sizeof(TableIdentifierT));
    strcpy(name, table_ptr->name);
    m_table.name = name;
  }
}


/**
 *
 */
TableScanner::~TableScanner() {
  for (size_t i=0; i<m_scan_spec.columns.size(); i++)
    delete [] m_scan_spec.columns[i];
  delete [] m_scan_spec.startRow;
  delete [] m_scan_spec.endRow;
  delete [] m_table.name;
}


bool TableScanner::next(CellT &cell) {
  int error;
  const ByteString32T *key, *value;
  Key keyComps;

  if (m_eos)
    return false;

  if (!m_started) {
    find_range_and_start_scan(m_scan_spec.startRow);
  }
  else if (!m_scanblock.more()) {
    if (m_scanblock.eos()) {
      std::string next_row = m_range_info.end_row;
      next_row.append(1,1);  // construct row key in next range
      find_range_and_start_scan(next_row.c_str());
    }
    else {
      if (m_fetch_outstanding) {
	if (!m_sync_handler.wait_for_reply(m_event_ptr)) {
	  LOG_VA_ERROR("RangeServer 'fetch scanblock' error : %s", Protocol::string_format_message(m_event_ptr).c_str());
	  throw Exception((int)Protocol::response_code(m_event_ptr));
	}
	else {
	  error = m_scanblock.load(m_event_ptr);
	  if (m_readahead && !m_scanblock.eos()) {
	    if ((error = m_range_server.fetch_scanblock(m_cur_addr, m_scanblock.get_scanner_id(), &m_sync_handler)) != Error::OK)
	      throw Exception(error);
	    m_fetch_outstanding = true;
	  }
	  else
	    m_fetch_outstanding = false;
	}
      }
      else {
	if ((error = m_range_server.fetch_scanblock(m_cur_addr, m_scanblock.get_scanner_id(), m_scanblock)) != Error::OK)
	  throw Exception(error);
      }
    }
  }

  // TODO: fix me ... scanblock could come back empty!
  assert(m_scanblock.more());

  if (m_scanblock.next(key, value)) {
    Schema::ColumnFamily *cf;
    cell.value = value->data;
    cell.value_len = value->len;
    if (!keyComps.load(key))
      throw Exception(Error::BAD_KEY);
    cell.row_key = keyComps.rowKey;
    cell.column_qualifier = keyComps.columnQualifier;

    if ((cf = m_schema_ptr->get_column_family(keyComps.columnFamily)) == 0) {
      // LOG ERROR ...
      throw Exception(Error::BAD_KEY);
    }
    cell.timestamp = keyComps.timestamp;
    return true;
  }
  else if (!m_scanblock.eos()) {    
    
  }
  
  /*
  1. next k/v pair in current scanblock
  2. next scanblock
  3. next tablet
  */  
  
  return false;
}



void TableScanner::find_range_and_start_scan(const char *row_key) {
  int error;

  if ((error = m_range_locator_ptr->find(&m_table, row_key, &m_range_info)) != Error::OK)
    throw Exception(error);
  m_started = true;

  m_cur_range.startRow = m_range_info.start_row.c_str();
  m_cur_range.endRow = m_range_info.end_row.c_str();

  if (!InetAddr::initialize(&m_cur_addr, m_range_info.location.c_str())) {
    LOG_VA_ERROR("Bad location info for table '%s' (id=%d) end row '%s' - %s",
		 m_table.name, m_table.id, m_range_info.end_row.c_str(), m_range_info.location.c_str());
    throw Exception(Error::INVALID_METADATA);
  }

  if ((error = m_range_server.create_scanner(m_cur_addr, m_table, m_cur_range, m_scan_spec, m_scanblock)) != Error::OK)
    throw Exception(error);

  // maybe kick off readahead
  if (m_readahead && !m_scanblock.eos()) {
    if ((error = m_range_server.fetch_scanblock(m_cur_addr, m_scanblock.get_scanner_id(), &m_sync_handler)) != Error::OK)
      throw Exception(error);
    m_fetch_outstanding = true;
  }
  else
    m_fetch_outstanding = false;
}
