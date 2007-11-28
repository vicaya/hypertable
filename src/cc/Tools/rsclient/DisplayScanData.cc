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

namespace Hypertable {

  void DisplayScanData(const ByteString32T *key, const ByteString32T *value, SchemaPtr &schemaPtr, bool display_values) {
    Key keyComps(key);
    Schema::ColumnFamily *cf;

    if (keyComps.flag == FLAG_DELETE_ROW) {
      cout << keyComps.timestamp << " " << keyComps.row << " DELETE" << endl;
    }
    else {
      if (keyComps.column_family_code > 0) {
	cf = schemaPtr->get_column_family(keyComps.column_family_code);
	if (keyComps.flag == FLAG_DELETE_CELL)
	  cout << keyComps.timestamp << " " << keyComps.row << " " << cf->name << ":" << keyComps.column_qualifier << " DELETE" << endl;
	else {
	  cout << keyComps.timestamp << " " << keyComps.row << " " << cf->name << ":" << keyComps.column_qualifier;
	  if (display_values)
	    cout << " " << (const char *)value->data;
	  cout << endl;
	}
      }
      else {
	cerr << "Bad column family (" << (int)keyComps.column_family_code << ") for row key " << keyComps.row;
	if (display_values)
	  cerr << " value=" << (const char *)value->data;
	cerr << endl;
      }
    }
    
  }

}
