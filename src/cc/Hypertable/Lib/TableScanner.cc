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

/** Cons
 */
TableScanner::TableScanner(SchemaPtr &schema_ptr, RangeLocatorPtr &range_locator_ptr, ScanSpecificationT &scan_spec) : m_schema_ptr(schema_ptr), m_range_locator_ptr(range_locator_ptr) {
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

  memcpy(&m_scan_spec.interval, &scan_spec.interval, sizeof(m_scan_spec.interval));
}


/**
 *
 */
TableScanner::~TableScanner() {
  for (size_t i=0; i<m_scan_spec.columns.size(); i++)
    delete [] m_scan_spec.columns[i];
  delete [] m_scan_spec.startRow;
  delete [] m_scan_spec.endRow;
}


bool TableScanner::next(CellT &cell) {
  const ByteString32T *key, *value;
  Key keyComps;

  if (m_eos)
    return false;

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

  

  
  /*  
  1. next k/v pair in current scanblock
  2. next scanblock
  3. next tablet
  */  
  
  return false;
}
