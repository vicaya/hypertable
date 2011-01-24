/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#include "ScanCells.h"

using namespace Hypertable;

bool ScanCells::load(EventPtr &event, SchemaPtr &schema,
                     const String &end_row, bool end_inclusive, int row_limit,
                     int *rows_seen, String &cur_row, CstrSet &rowset,
                     int *scanner_id, bool *eos, int64_t *bytes_scanned) {
  SerializedKey serkey;
  ByteString value;
  Key key;

  HT_ASSERT(!m_loaded);
  m_loaded = true;
  m_scanblock.load(event);

  *eos = m_scanblock.eos();
  *scanner_id = m_scanblock.get_scanner_id();
  while (m_scanblock.next(serkey, value)) {
    Schema::ColumnFamily *cf;
    Cell cell;
    if (!key.load(serkey))
      HT_THROW(Error::BAD_KEY, "");

    // check for end row
    if (!strcmp(key.row, Key::END_ROW_MARKER)) {
      return true;
    }

    if (end_inclusive) {
      if (strcmp(key.row, end_row.c_str()) > 0) {
        return true;
      }
    }
    else {
      if (strcmp(key.row, end_row.c_str()) >= 0) {
        return true;
      }
    }

    // check for row change and row limit
    if (strcmp(cur_row.c_str(), key.row)) {
      (*rows_seen)++;
      cur_row = key.row;
      if (row_limit > 0 && *rows_seen > row_limit) {
        return true;
      }
    }

    cell.row_key = key.row;
    cell.column_qualifier = key.column_qualifier;
    if ((cf = schema->get_column_family(key.column_family_code)) == 0) {
      if (key.flag != FLAG_DELETE_ROW)
	      HT_THROWF(Error::BAD_KEY, "Unexpected column family code %d",
		  (int)key.column_family_code);
      cell.column_family = "";
    }
    else
      cell.column_family = cf->name.c_str();

    cell.timestamp = key.timestamp;
    cell.revision = key.revision;
    cell.value_len = value.decode_length(&cell.value);
    cell.flag = key.flag;
    m_cells.add(cell, false);
    *bytes_scanned += key.length + cell.value_len;

    // if rowset scan remove scanned row
    while (!rowset.empty() && strcmp(*rowset.begin(), key.row) < 0)
      rowset.erase(rowset.begin());
  }
  return false;
}

