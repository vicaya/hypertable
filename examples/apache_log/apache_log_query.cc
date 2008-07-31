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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
#include "Common/Compat.h"

#include <cstdio>

#include "Common/Error.h"
#include "Common/System.h"

#include "Hypertable/Lib/Client.h"

using namespace Hypertable;
using namespace std;

namespace {

  const char *usage = 
    "\n"
    "  usage: apache_log_query <row-prefix> [ <cf> ... ]\n"
    "\n"
    "  Queries the table 'LogDb' for all rows that start\n"
    "  with <row-prefix>.  If no column families are\n"
    "  specified, then all column families are returned.\n"
    "  Otherwise, just the column families specified by\n"
    "  the <cf> arguments are returned\n";
}

/**
 * This program is designed to query the table 'LogDb' for
 * the contents of the row that has the key equal to the web page
 * supplied as the command line argument.  The 'LogDb' table
 * is populated with the apache_log_load program.
 */
int main(int argc, char **argv) {
  ClientPtr client_ptr;
  TablePtr table_ptr;
  TableScannerPtr scanner_ptr;
  ScanSpecBuilder scan_spec_builder;
  Cell cell;
  String end_row;

  if (argc <= 1) { 
    cout << usage << endl;
    return 0;
  }

  try {

    // Create Hypertable client object
    client_ptr = new Client( System::locate_install_dir(argv[0]) );

    // Open the 'LogDb' table
    table_ptr = client_ptr->open_table("LogDb");

    // setup row interval
    end_row = (String)argv[1];
    end_row.append(1, 0xff);  // next minimum row
    scan_spec_builder.add_row_interval(argv[1], true, end_row, false);

    // setup scan_spec columns
    for (int i=2; i<argc; i++)
      scan_spec_builder.add_column(argv[i]);

    // Create a scanner on the 'LogDb' table 
    scanner_ptr = table_ptr->create_scanner(scan_spec_builder);

  }
  catch (std::exception &e) {
    cerr << "error: " << e.what() << endl;
    return 1;
  }

  // Iterate through the cells returned by the scanner
  while (scanner_ptr->next(cell)) {
    printf("%s\t%s", cell.row_key, cell.column_family);
    if (*cell.column_qualifier)
      printf(":%s", cell.column_qualifier);
    printf("\t%s\n", std::string((const char *)cell.value, cell.value_len).c_str());
  }

  return 0;
}
