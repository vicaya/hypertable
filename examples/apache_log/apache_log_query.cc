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

#include <cstdio>

#include "Common/Error.h"
#include "Common/System.h"

#include "Hypertable/Lib/Client.h"

using namespace Hypertable;
using namespace std;

/**
 * This program is designed to query the table 'WebServerLog' for
 * the contents of the row that has the key equal to the web page
 * supplied as the command line argument.  The 'WebServerLog' table
 * is populated with the apache_log_load program.
 */
int main(int argc, char **argv) {
  int error;
  ClientPtr hypertable_client_ptr;
  TablePtr table_ptr;
  TableScannerPtr scanner_ptr;
  ScanSpecificationT scan_spec;
  CellT cell;
  uint32_t nsec;
  time_t unix_time;
  struct tm tms;

  if (argc <= 1) { 
    cout << "usage: apache_log_query <page>" << endl;
    return 0;
  }

  try {

    // Create Hypertable client object
    hypertable_client_ptr = new Client(argv[0]);

    // Open the 'WebServerLog' table
    if ((error = hypertable_client_ptr->open_table("WebServerLog", table_ptr)) != Error::OK) {
      cerr << "Error: unable to open table 'WebServerLog' - " << Error::get_text(error) << endl;
      return 1;
    }

    // Set up the scan specification
    scan_spec.rowLimit = 1;
    scan_spec.max_versions = 0;
    scan_spec.startRow = scan_spec.endRow = argv[1];
    scan_spec.startRowInclusive = scan_spec.endRowInclusive = true;
    scan_spec.interval.first = scan_spec.interval.second = 0;  // 0 means no time predicate
    scan_spec.return_deletes = false;

    // Create a scanner on the 'WebServerLog' table 
    if ((error = table_ptr->create_scanner(scan_spec, scanner_ptr)) != Error::OK) {
      cerr << "Error: problem creating scanner on table 'WebServerLog' - " << Error::get_text(error) << endl;
      return 1;
    }
  }
  catch (std::exception &e) {
    cerr << "error: " << e.what() << endl;
    return 1;
  }

  // Iterate through the cells returned by the scanner
  while (scanner_ptr->next(cell)) {
    nsec = cell.timestamp % 1000000000LL;
    unix_time = cell.timestamp / 1000000000LL;
    localtime_r(&unix_time, &tms);
    printf("%d-%02d-%02d %02d:%02d:%02d.%09d", tms.tm_year+1900, tms.tm_mon+1, tms.tm_mday, tms.tm_hour, tms.tm_min, tms.tm_sec, nsec);
    printf("\t%s\t%s", cell.row_key, cell.column_family);
    if (*cell.column_qualifier)
      printf(":%s", cell.column_qualifier);
    printf("\t%s\n", std::string((const char *)cell.value, cell.value_len).c_str());
  }

  return 0;
}
