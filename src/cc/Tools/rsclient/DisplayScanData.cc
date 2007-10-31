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
#include <iostream>

#include "Hypertable/Lib/Key.h"

#include "DisplayScanData.h"

namespace hypertable {

  void DisplayScanData(const ByteString32T *key, const ByteString32T *value, SchemaPtr &schemaPtr) {
    Key keyComps(key);
    Schema::ColumnFamily *cf;

    if (keyComps.flag == FLAG_DELETE_ROW) {
      cout << keyComps.timestamp << " " << keyComps.rowKey << " DELETE" << endl;
    }
    else {
      if (keyComps.columnFamily > 0) {
	cf = schemaPtr->GetColumnFamily(keyComps.columnFamily);
	if (keyComps.flag == FLAG_DELETE_CELL)
	  cout << keyComps.timestamp << " " << keyComps.rowKey << " " << cf->name << ":" << keyComps.columnQualifier << " DELETE" << endl;
	else
	  cout << keyComps.timestamp << " " << keyComps.rowKey << " " << cf->name << ":" << keyComps.columnQualifier << endl;
      }
      else {
	cerr << "Bad column family (" << (int)keyComps.columnFamily << ") for row key " << keyComps.rowKey << endl;
      }
    }
    
  }

}
