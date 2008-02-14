/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include <string>
#include <vector>

#include "Defaults.h"
#include "Key.h"
#include "TableScanner.h"

using namespace Hypertable;

namespace {
  const char end_row_key[2] = { (char)0xff, (char)0xff };
}

/**
 * TODO: Asynchronously destroy dangling scanners on EOS
 */

/**
 *
 */
TableScanner::TableScanner(PropertiesPtr &props_ptr, Comm *comm, TableIdentifierT *table_identifier, SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr, ScanSpecificationT &scan_spec) : m_comm(comm), m_schema_ptr(schema_ptr), m_range_locator_ptr(range_locator_ptr), m_range_server(comm, HYPERTABLE_RANGESERVER_CLIENT_TIMEOUT), m_table_name(table_identifier->name), m_started(false), m_eos(false), m_readahead(true), m_fetch_outstanding(false), m_rows_seen(0) {
  char *str;
  time_t client_timeout;

  if ((client_timeout = props_ptr->get_int("Hypertable.Request.Timeout", 0)) != 0)
    m_range_server.set_timeout(client_timeout);

  m_scan_spec.rowLimit = scan_spec.rowLimit;
  m_scan_spec.max_versions = scan_spec.max_versions;

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

  m_scan_spec.return_deletes = scan_spec.return_deletes;

  // copy TableIdentifierT
  memcpy(&m_table_identifier, table_identifier, sizeof(TableIdentifierT));
  m_table_identifier.name = m_table_name.c_str();

}


/**
 *
 */
TableScanner::~TableScanner() {
  for (size_t i=0; i<m_scan_spec.columns.size(); i++)
    delete [] m_scan_spec.columns[i];
  delete [] m_scan_spec.startRow;
  delete [] m_scan_spec.endRow;

  // if there is an outstanding fetch, wait for it to come back or timeout
  if (m_fetch_outstanding)
    m_sync_handler.wait_for_reply(m_event_ptr);
}



bool TableScanner::next(CellT &cell) {
  int error;
  const ByteString32T *key, *value;
  Key keyComps;

  if (m_eos)
    return false;

  if (!m_started)
    find_range_and_start_scan(m_scan_spec.startRow);

  while (!m_scanblock.more()) {
    if (m_scanblock.eos()) {
      if (!strcmp(m_range_info.end_row.c_str(), Key::END_ROW_MARKER) ||
	  (m_scan_spec.endRow && (strcmp(m_scan_spec.endRow, m_range_info.end_row.c_str()) <= 0))) {
	m_range_server.destroy_scanner(m_cur_addr, m_scanblock.get_scanner_id(), 0);
	m_eos = true;
	return false;
      }
      std::string next_row = m_range_info.end_row;
      next_row.append(1,1);  // construct row key in next range
      find_range_and_start_scan(next_row.c_str());
    }
    else {
      if (m_fetch_outstanding) {
	if (!m_sync_handler.wait_for_reply(m_event_ptr)) {
	  m_fetch_outstanding = false;
	  HT_ERRORF("RangeServer 'fetch scanblock' error : %s", Protocol::string_format_message(m_event_ptr).c_str());
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

  if (m_scanblock.next(key, value)) {
    Schema::ColumnFamily *cf;
    if (!keyComps.load(key))
      throw Exception(Error::BAD_KEY);

    // check for end row
    if (m_scan_spec.endRow) {
      if (m_scan_spec.endRowInclusive) {
	if (strcmp(keyComps.row, m_scan_spec.endRow) > 0) {
	  m_range_server.destroy_scanner(m_cur_addr, m_scanblock.get_scanner_id(), 0);
	  m_eos = true;
	  return false;
	}
      }
      else {
	if (strcmp(keyComps.row, m_scan_spec.endRow) >= 0) {
	  m_range_server.destroy_scanner(m_cur_addr, m_scanblock.get_scanner_id(), 0);
	  m_eos = true;
	  return false;
	}
      }
    }
    else if (!strcmp(keyComps.row, end_row_key)) {
      m_range_server.destroy_scanner(m_cur_addr, m_scanblock.get_scanner_id(), 0);
      m_eos = true;
      return false;
    }

    // check for row change and row limit
    if (strcmp(m_cur_row.c_str(), keyComps.row)) {
      m_rows_seen++;
      m_cur_row = keyComps.row;
      if (m_scan_spec.rowLimit > 0 && m_rows_seen > m_scan_spec.rowLimit) {
	m_range_server.destroy_scanner(m_cur_addr, m_scanblock.get_scanner_id(), 0);
	m_eos = true;
	return false;
      }
    }

    cell.row_key = keyComps.row;
    cell.column_qualifier = keyComps.column_qualifier;
    if ((cf = m_schema_ptr->get_column_family(keyComps.column_family_code)) == 0) {
      // LOG ERROR ...
      //throw Exception(Error::BAD_KEY);
      cell.column_family = 0;
    }
    else
      cell.column_family = cf->name.c_str();
    cell.timestamp = keyComps.timestamp;
    cell.value = value->data;
    cell.value_len = value->len;
    cell.flag = keyComps.flag;
    return true;
  }

  HT_ERROR("No end marker found at end of table.");

  m_range_server.destroy_scanner(m_cur_addr, m_scanblock.get_scanner_id(), 0);
  m_eos = true;
  return false;
}



void TableScanner::find_range_and_start_scan(const char *row_key) {
  int     error;
  RangeT  range;
  DynamicBuffer dbuf(0);

  if ((error = m_range_locator_ptr->find(&m_table_identifier, row_key, &m_range_info, false)) != Error::OK) {
    // try again, the hard way
    if ((error = m_range_locator_ptr->find(&m_table_identifier, row_key, &m_range_info, 21)) != Error::OK)
      throw Exception(error, std::string("Unable to find range server for table '") + m_table_identifier.name + "' row key '" + (row_key ? row_key : "") + "'");
  }

  m_started = true;

  dbuf.ensure(m_range_info.start_row.length() + m_range_info.end_row.length() + 2);
  range.startRow = (const char *)dbuf.addNoCheck(m_range_info.start_row.c_str(), m_range_info.start_row.length()+1);
  range.endRow   = (const char *)dbuf.addNoCheck(m_range_info.end_row.c_str(), m_range_info.end_row.length()+1);
  if (!LocationCache::location_to_addr(m_range_info.location.c_str(), m_cur_addr)) {
    HT_ERRORF("Invalid location found in METADATA entry range [%s..%s] - %s",
		 range.startRow, range.endRow, m_range_info.location.c_str());
    throw Exception(Error::INVALID_METADATA);
  }

  if ((error = m_range_server.create_scanner(m_cur_addr, m_table_identifier, range, m_scan_spec, m_scanblock)) != Error::OK) {

    // try again, the hard way
    if ((error = m_range_locator_ptr->find(&m_table_identifier, row_key, &m_range_info, 21)) != Error::OK)
      throw Exception(error, std::string("Unable to find range server for table '") + m_table_identifier.name + "' row key '" + (row_key ? row_key : "") + "'");

    // reset the range information
    dbuf.ensure(m_range_info.start_row.length() + m_range_info.end_row.length() + 2);
    range.startRow = (const char *)dbuf.addNoCheck(m_range_info.start_row.c_str(), m_range_info.start_row.length()+1);
    range.endRow   = (const char *)dbuf.addNoCheck(m_range_info.end_row.c_str(), m_range_info.end_row.length()+1);
    if (!LocationCache::location_to_addr(m_range_info.location.c_str(), m_cur_addr)) {
      HT_ERRORF("Invalid location found in METADATA entry range [%s..%s] - %s",
		   range.startRow, range.endRow, m_range_info.location.c_str());
      throw Exception(Error::INVALID_METADATA);
    }

    // create the scanner
    if ((error = m_range_server.create_scanner(m_cur_addr, m_table_identifier, range, m_scan_spec, m_scanblock)) != Error::OK)
      throw Exception(error, std::string("Problem creating scanner on ") + m_table_identifier.name + "[" + range.startRow + ".." + range.endRow + "]");
  }

  // maybe kick off readahead
  if (m_readahead && !m_scanblock.eos()) {
    if ((error = m_range_server.fetch_scanblock(m_cur_addr, m_scanblock.get_scanner_id(), &m_sync_handler)) != Error::OK)
      throw Exception(error);
    m_fetch_outstanding = true;
  }
  else
    m_fetch_outstanding = false;
}
