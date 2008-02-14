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

#include <cctype>
#include <cstdlib>
#include <cstring>
#include <iostream>

extern "C" {
#include <regex.h>
}

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Event.h"

#include "Common/Error.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/ScanBlock.h"

#include "CommandCreateScanner.h"
#include "DisplayScanData.h"
#include "ParseRangeSpec.h"

using namespace Hypertable;
using namespace std;

const char *CommandCreateScanner::ms_usage[] = {
  "create scanner <range> [OPTIONS]",
  "",
  "  OPTIONS:",
  "  row-limit=<n>   Limit the number of rows returned to <n>",
  "  max-versions=<n>  Limit the number of cell versions returned to <n>",
  "  start=<row>     Start scan at row <row>",
  "  end=<row>       End scan at row <row>, non inclusive",
  "  row-range=<rng> Row range of form '('|'[' start, end ']'|')'",
  "  columns=<column1>[,<column2>...]  Only return these columns",
  "  start-time=<t>  Only return cells whose timestamp is >= <t>",
  "  end-time=<t>    Only return cells whose timestamp is < <t>",
  "  display-values  Cast values to a (char *) and display",
  "",
  "  This command issues a CREATE SCANNER command to the range server.",
  (const char *)0
};

int32_t           CommandCreateScanner::ms_scanner_id = -1;
TableInfo        *CommandCreateScanner::ms_table_info = 0;
bool              CommandCreateScanner::ms_display_values = false;

int CommandCreateScanner::run() {
  int error;
  std::string tableName;
  std::string startRow;
  std::string endRow;
  RangeT range;
  ScanSpecificationT scan_spec;
  char *last;
  const char *columnStr;
  ScanBlock scanblock;
  SchemaPtr schema_ptr;
  TableIdentifierT *table;

  if (m_args.size() == 0) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if (m_args[0].second != "" || !ParseRangeSpec(m_args[0].first, tableName, startRow, endRow)) {
    cerr << "Invalid range specification." << endl;
    return -1;
  }

  ms_table_info = TableInfo::map[tableName];

  if (ms_table_info == 0) {
    ms_table_info = new TableInfo(tableName);
    if ((error = ms_table_info->load(m_hyperspace_ptr)) != Error::OK)
      return error;
    TableInfo::map[tableName] = ms_table_info;
  }

  table = ms_table_info->get_table_identifier();
  ms_table_info->get_schema_ptr(schema_ptr);

  /**
  cout << "TableName  = " << tableName << endl;
  //cout << "TableId    = " << table->id << endl;
  //cout << "Generation = " << table->generation << endl;
  cout << "StartRow   = " << startRow << endl;
  cout << "EndRow     = " << endRow << endl;
  **/

  range.startRow = startRow.c_str();
  range.endRow = endRow.c_str();

  /**
   * Create Scan specification
   */
  scan_spec.rowLimit = 0;
  scan_spec.max_versions = 0;
  scan_spec.columns.clear();
  scan_spec.startRow = 0;
  scan_spec.startRowInclusive = true;
  scan_spec.endRow = 0;
  scan_spec.endRowInclusive = true;
  scan_spec.interval.first = 0;
  scan_spec.interval.second = 0;
  scan_spec.return_deletes = false;

  for (size_t i=1; i<m_args.size(); i++) {
    if (m_args[i].second == "") {
      cerr << "No value supplied for '" << m_args[i].first << "'" << endl;
      return -1;
    }
    if (m_args[i].first == "row-limit")
      scan_spec.rowLimit = atoi(m_args[i].second.c_str());
    else if (m_args[i].first == "max-versions")
      scan_spec.max_versions = atoi(m_args[i].second.c_str());
    else if (m_args[i].first == "start")
      scan_spec.startRow = m_args[i].second.c_str();
    else if (m_args[i].first == "end")
      scan_spec.endRow = m_args[i].second.c_str();
    else if (m_args[i].first == "row-range") {
      if (!decode_row_range_spec(m_args[i].second, scan_spec))
	return -1;
    }
    else if (m_args[i].first == "start-time")
      scan_spec.interval.first = strtoll(m_args[i].second.c_str(), 0, 10);
    else if (m_args[i].first == "end-time")
      scan_spec.interval.second = strtoll(m_args[i].second.c_str(), 0, 10);
    else if (m_args[i].first == "display-values")
      ms_display_values = true;
    else if (m_args[i].first == "columns") {
      columnStr = strtok_r((char *)m_args[i].second.c_str(), ",", &last);
      while (columnStr) {
	scan_spec.columns.push_back(columnStr);
	columnStr = strtok_r(0, ",", &last);
      }
    }
  }

  /**
   * 
   */
  if ((error = m_range_server_ptr->create_scanner(m_addr, *table, range, scan_spec, scanblock)) != Error::OK)
    return error;

  ms_scanner_id = scanblock.get_scanner_id();

  const ByteString32T *key, *value;

  while (scanblock.next(key, value))
    DisplayScanData(key, value, schema_ptr, ms_display_values);

  if (scanblock.eos())
    ms_scanner_id = -1;

  return Error::OK;
}


/**
 *
 */
  bool CommandCreateScanner::decode_row_range_spec(std::string &specStr, ScanSpecificationT &scan_spec) {
  regex_t reg;
  regmatch_t pmatch[5];
  int error;
  char errBuf[256];
  char *base = (char *)specStr.c_str();

  errBuf[0] = 0;

  error = regcomp(&reg, "\\([([]\\)\\([^,]*\\),\\([^)]*\\)\\([])]\\)", 0);
  if (error != 0) {
    regerror(error, &reg, errBuf, 256);
    goto abort;
  }

  error = regexec(&reg, base, 5, pmatch, 0);
  if (error != 0) {
    regerror(error, &reg, errBuf, 256);
    goto abort;
  }

  for (size_t i=0; i<5; i++) {
    if (pmatch[i].rm_so == -1)
      goto abort;
  }

  if (base[pmatch[1].rm_so] == '(')
    scan_spec.startRowInclusive = false;
  else
    scan_spec.startRowInclusive = true;

  if (base[pmatch[4].rm_so] == ')')
    scan_spec.endRowInclusive = false;
  else
    scan_spec.endRowInclusive = true;

  /**
   * Start Row
   */
  while (pmatch[2].rm_so < pmatch[2].rm_eo && isspace(base[pmatch[2].rm_so]))
    pmatch[2].rm_so++;

  while (pmatch[2].rm_so < pmatch[2].rm_eo && isspace(base[pmatch[2].rm_eo-1]))
    pmatch[2].rm_eo--;

  if (pmatch[2].rm_so < pmatch[2].rm_eo) {
    scan_spec.startRow = &base[pmatch[2].rm_so];
    base[pmatch[2].rm_eo] = 0;
  }
  else
    scan_spec.startRow = 0;

  /**
   * End Row
   */
  while (pmatch[3].rm_so < pmatch[3].rm_eo && isspace(base[pmatch[3].rm_so]))
    pmatch[3].rm_so++;

  while (pmatch[3].rm_so < pmatch[3].rm_eo && isspace(base[pmatch[3].rm_eo-1]))
    pmatch[3].rm_eo--;

  if (pmatch[3].rm_so < pmatch[3].rm_eo) {
    scan_spec.endRow = &base[pmatch[3].rm_so];
    base[pmatch[3].rm_eo] = 0;
  }
  else
    scan_spec.endRow = 0;

  regfree(&reg);

  return true;

 abort:
  if (errBuf[0] == 0)
    cerr << "Invalid row range specification." << endl;
  else
    cerr << "regex: " << errBuf << endl;
  return false;
}
