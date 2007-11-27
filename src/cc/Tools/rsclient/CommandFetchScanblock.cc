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

#include <cstdlib>
#include <cstring>
#include <iostream>

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Event.h"

#include "Common/Error.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/ScanBlock.h"

#include "CommandCreateScanner.h"
#include "CommandFetchScanblock.h"
#include "DisplayScanData.h"
#include "ParseRangeSpec.h"

using namespace Hypertable;
using namespace std;

const char *CommandFetchScanblock::ms_usage[] = {
  "fetch scanblock <scanner-id>",
  "",
  "  This command issues a FETCH SCANBLOCK command to the range server.  If no",
  "  <scanner-id> argument is supplied, it uses the cached scanner ID from the",
  "  most recently executed CREATE SCANNER command.",
  (const char *)0
};



int CommandFetchScanblock::run() {
  int error;
  ScanBlock scanblock;
  int32_t scannerId = -1;
  SchemaPtr schema_ptr;

  if (m_args.size() > 1) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if (CommandCreateScanner::ms_table_info == 0) {
    cerr << "No scanner information found, must run \"create table\" first" << endl;
    return -1;
  }

  if (m_args.size() > 0) {
    if (m_args[0].second != "") {
      cerr << "Invalid scanner ID." << endl;
      return -1;
    }
    scannerId = atoi(m_args[0].first.c_str());
  }
  else
    scannerId = CommandCreateScanner::ms_scanner_id;

  CommandCreateScanner::ms_table_info->get_schema_ptr(schema_ptr);

  /**
   * 
   */
  if ((error = m_range_server_ptr->fetch_scanblock(m_addr, scannerId, scanblock)) != Error::OK)
    return error;

  CommandCreateScanner::ms_scanner_id = scanblock.get_scanner_id();

  const ByteString32T *key, *value;

  while (scanblock.next(key, value))
    DisplayScanData(key, value, schema_ptr, CommandCreateScanner::ms_display_values);

  if (scanblock.eos())
    CommandCreateScanner::ms_scanner_id = -1;

  return Error::OK;
}
