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
#include "TableInfo.h"

using namespace Hypertable;
using namespace std;

const char *CommandLoadRange::ms_usage[] = {
  "load range <range>",
  "",
  "  This command issues a LOAD RANGE command to the range server.",
  (const char *)0
};

int CommandLoadRange::run() {
  int error;
  std::string tableName;
  std::string startRow;
  std::string endRow;
  TableIdentifierT *table;
  RangeT range;
  TableInfo *tableInfo;

  if (m_args.size() != 1) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if (m_args[0].second != "")
    Usage::dump_and_exit(ms_usage);

  if (!ParseRangeSpec(m_args[0].first, tableName, startRow, endRow)) {
    cerr << "error:  Invalid range specification." << endl;
  }

  tableInfo = TableInfo::map[tableName];

  if (tableInfo == 0) {
    tableInfo = new TableInfo(tableName);
    if ((error = tableInfo->load(m_hyperspace_ptr)) != Error::OK)
      return error;
    TableInfo::map[tableName] = tableInfo;
  }
  table = tableInfo->get_table_identifier();

  cout << "TableName  = " << tableName << endl;
  //cout << "TableId    = " << table->id << endl;
  //cout << "Generation = " << table->generation << endl;
  cout << "StartRow   = " << startRow << endl;
  cout << "EndRow     = " << endRow << endl;

  range.startRow = startRow.c_str();
  range.endRow = endRow.c_str();

  if ((error = m_range_server_ptr->load_range(m_addr, *table, range, 200000000LL, 0)) != Error::OK)
    return error;

  return Error::OK;
}
