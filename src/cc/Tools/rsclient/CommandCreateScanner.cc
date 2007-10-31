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

#include "Hypertable/Lib/ScanResult.h"

#include "CommandCreateScanner.h"
#include "DisplayScanData.h"
#include "ParseRangeSpec.h"
#include "FetchSchema.h"
#include "Global.h"

using namespace hypertable;
using namespace std;

const char *CommandCreateScanner::msUsage[] = {
  "create scanner <range> [OPTIONS]",
  "",
  "  OPTIONS:",
  "  row-limit=<n>   Limit the number of rows returned to <n>",
  "  cell-limit=<n>  Limit the number of cell versions returned to <n>",
  "  start=<row>     Start scan at row <row>",
  "  end=<row>       End scan at row <row>, non inclusive",
  "  row-range=<rng> Row range of form '('|'[' start, end ']'|')'",
  "  columns=<column1>[,<column2>...]  Only return these columns",
  "  start-time=<t>  Only return cells whose timestamp is >= <t>",
  "  end-time=<t>    Only return cells whose timestamp is < <t>",
  "",
  "  This command issues a CREATE SCANNER command to the range server.",
  (const char *)0
};



int CommandCreateScanner::run() {
  off_t len;
  int error;
  std::string tableName;
  std::string startRow;
  std::string endRow;
  RangeSpecificationT rangeSpec;
  ScanSpecificationT scanSpec;
  char *last;
  const char *columnStr;
  uint32_t ilen;
  ScanResult result;

  if (mArgs.size() == 0) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if (mArgs[0].second != "" || !ParseRangeSpec(mArgs[0].first, tableName, startRow, endRow)) {
    cerr << "Invalid range specification." << endl;
    return -1;
  }

  Global::outstandingSchemaPtr = Global::schemaMap[tableName];

  if (!Global::outstandingSchemaPtr) {
    if ((error = FetchSchema(tableName, Global::hyperspace, Global::outstandingSchemaPtr)) != Error::OK)
      return error;
    Global::schemaMap[tableName] = Global::outstandingSchemaPtr;    
  }

  cout << "Generation = " << Global::outstandingSchemaPtr->GetGeneration() << endl;
  cout << "TableName  = " << tableName << endl;
  cout << "StartRow   = " << startRow << endl;
  cout << "EndRow     = " << endRow << endl;

  rangeSpec.tableName = tableName.c_str();
  rangeSpec.startRow = startRow.c_str();
  rangeSpec.endRow = endRow.c_str();
  rangeSpec.generation = Global::outstandingSchemaPtr->GetGeneration();

  /**
   * Create Scan specification
   */
  scanSpec.rowLimit = 0;
  scanSpec.cellLimit = 0;
  scanSpec.columns.clear();
  scanSpec.startRow = 0;
  scanSpec.startRowInclusive = true;
  scanSpec.endRow = 0;
  scanSpec.endRowInclusive = true;
  scanSpec.interval.first = 0;
  scanSpec.interval.second = 0;

  for (size_t i=1; i<mArgs.size(); i++) {
    if (mArgs[i].second == "") {
      cerr << "No value supplied for '" << mArgs[i].first << "'" << endl;
      return -1;
    }
    if (mArgs[i].first == "row-limit")
      scanSpec.rowLimit = atoi(mArgs[i].second.c_str());
    else if (mArgs[i].first == "cell-limit")
      scanSpec.cellLimit = atoi(mArgs[i].second.c_str());
    else if (mArgs[i].first == "start")
      scanSpec.startRow = mArgs[i].second.c_str();
    else if (mArgs[i].first == "end")
      scanSpec.endRow = mArgs[i].second.c_str();
    else if (mArgs[i].first == "row-range") {
      if (!DecodeRowRangeSpec(mArgs[i].second, scanSpec))
	return -1;
    }
    else if (mArgs[i].first == "start-time")
      scanSpec.interval.first = strtoll(mArgs[i].second.c_str(), 0, 10);
    else if (mArgs[i].first == "end-time")
      scanSpec.interval.second = strtoll(mArgs[i].second.c_str(), 0, 10);
    else if (mArgs[i].first == "columns") {
      columnStr = strtok_r((char *)mArgs[i].second.c_str(), ",", &last);
      while (columnStr) {
	scanSpec.columns.push_back(columnStr);
	columnStr = strtok_r(0, ",", &last);
      }
    }
  }

  /**
   * 
   */
  if ((error = Global::rangeServer->CreateScanner(mAddr, rangeSpec, scanSpec, result)) != Error::OK)
    return error;

  Global::outstandingScannerId = result.GetId();

  ScanResult::VectorT &rvec = result.GetVector();

  for (size_t i=0; i<rvec.size(); i++)
    DisplayScanData(rvec[i].first, rvec[i].second, Global::outstandingSchemaPtr);

  if (result.Eos())
    Global::outstandingScannerId = -1;

  return Error::OK;
}


/**
 *
 */
bool CommandCreateScanner::DecodeRowRangeSpec(std::string &specStr, ScanSpecificationT &scanSpec) {
  regex_t reg;
  regmatch_t pmatch[5];
  int error;
  char errBuf[256];
  char *base = (char *)specStr.c_str();
  const char *ptr;

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
    scanSpec.startRowInclusive = false;
  else
    scanSpec.startRowInclusive = true;

  if (base[pmatch[4].rm_so] == ')')
    scanSpec.endRowInclusive = false;
  else
    scanSpec.endRowInclusive = true;

  /**
   * Start Row
   */
  while (pmatch[2].rm_so < pmatch[2].rm_eo && isspace(base[pmatch[2].rm_so]))
    pmatch[2].rm_so++;

  while (pmatch[2].rm_so < pmatch[2].rm_eo && isspace(base[pmatch[2].rm_eo-1]))
    pmatch[2].rm_eo--;

  if (pmatch[2].rm_so < pmatch[2].rm_eo) {
    scanSpec.startRow = &base[pmatch[2].rm_so];
    base[pmatch[2].rm_eo] = 0;
  }
  else
    scanSpec.startRow = 0;

  /**
   * End Row
   */
  while (pmatch[3].rm_so < pmatch[3].rm_eo && isspace(base[pmatch[3].rm_so]))
    pmatch[3].rm_so++;

  while (pmatch[3].rm_so < pmatch[3].rm_eo && isspace(base[pmatch[3].rm_eo-1]))
    pmatch[3].rm_eo--;

  if (pmatch[3].rm_so < pmatch[3].rm_eo) {
    scanSpec.endRow = &base[pmatch[3].rm_so];
    base[pmatch[3].rm_eo] = 0;
  }
  else
    scanSpec.endRow = 0;

  regfree(&reg);

  return true;

 abort:
  if (errBuf[0] == 0)
    cerr << "Invalid row range specification." << endl;
  else
    cerr << "regex: " << errBuf << endl;
  return false;
}
