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

#include <cstdlib>
#include <cstring>
#include <iostream>

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Event.h"

#include "Common/Error.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/ScanBlock.h"

#include "CommandCreateScanner.h"
#include "CommandDestroyScanner.h"
#include "DisplayScanData.h"
#include "ParseRangeSpec.h"

using namespace Hypertable;
using namespace std;

const char *CommandDestroyScanner::ms_usage[] = {
  "destroy scanner [<scanner-id>]",
  "",
  "  This command issues a DESTROY SCANNER command to the range server.  If no",
  "  <scanner-id> argument is supplied, it uses the cached scanner ID from the",
  "  most recently executed CREATE SCANNER command.",
  (const char *)0
};



int CommandDestroyScanner::run() {
  int32_t scannerId = -1;

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

  return m_range_server_ptr->destroy_scanner(m_addr, scannerId);
}

