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

#include "Common/Error.h"
#include "Common/Usage.h"

#include "CommandLoadRange.h"
#include "ParseRangeSpec.h"
#include "FetchSchema.h"
#include "Global.h"

using namespace hypertable;
using namespace std;

const char *CommandLoadRange::ms_usage[] = {
  "load range <range>",
  "",
  "  This command issues a LOAD RANGE command to the range server.",
  (const char *)0
};

int CommandLoadRange::run() {
  off_t len;
  const char *schema = 0;
  int error;
  std::string tableName;
  std::string startRow;
  std::string endRow;
  RangeSpecificationT rangeSpec;
  SchemaPtr schemaPtr;

  if (m_args.size() != 1) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if (m_args[0].second != "")
    Usage::dump_and_exit(ms_usage);

  if (!ParseRangeSpec(m_args[0].first, tableName, startRow, endRow)) {
    cerr << "error:  Invalid range specification." << endl;
  }

  schemaPtr = Global::schemaMap[tableName];

  if (!schemaPtr) {
    if ((error = FetchSchema(tableName, Global::hyperspace, schemaPtr)) != Error::OK)
      return error;
    Global::schemaMap[tableName] = schemaPtr;    
  }

  cout << "Generation = " << schemaPtr->get_generation() << endl;
  cout << "TableName  = " << tableName << endl;
  cout << "StartRow   = " << startRow << endl;
  cout << "EndRow     = " << endRow << endl;

  rangeSpec.tableName = tableName.c_str();
  rangeSpec.startRow = startRow.c_str();
  rangeSpec.endRow = endRow.c_str();
  rangeSpec.generation = schemaPtr->get_generation();

  if ((error = Global::rangeServer->load_range(m_addr, rangeSpec)) != Error::OK)
    return error;

  return Error::OK;
}
