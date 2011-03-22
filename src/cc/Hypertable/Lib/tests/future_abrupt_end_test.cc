/**
 * Copyright (C) 2011 Hypertable, Inc.
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>
#include <algorithm>
#include <fstream>

extern "C" {
#include <unistd.h>
}

#include "Common/StringExt.h"
#include "Common/Init.h"
#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/ServerLauncher.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/Future.h"
#include "Hypertable/Lib/KeySpec.h"

#include "AsyncComm/ReactorFactory.h"

using namespace Hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: ScannerCancelTest key_size num_cells num_scanners intervals_per_scanner",
    "This test creates asynchronous scanners on the LoadTest table and destroys the scanner before it ",
    "completes, as soon once it reads num_cells cells.",
    "key_size is the zero-padded length of the integer key strings",
    "num_cells is the total number of cells scanned",
    "num_scanners is the number of asynchronous scanners to be launched",
    "intervals_per_scanner is the number if row intervals per async scanner"
    "",
    0
  };

}


int main(int argc, char **argv) {
  ClientPtr hypertable_client_ptr;
  NamespacePtr namespace_ptr;
  TablePtr table_ptr;
  ScanSpecBuilder ssbuilder;
  ScanSpec scan_spec;
  Cell cell;
  size_t num_cells, num_scanners, intervals_per_scanner, key_size;
  size_t ii, jj;

  if ((argc > 1 && (!strcmp(argv[1], "-?") || !strcmp(argv[1], "--help"))) || argc != 5)
    Usage::dump_and_exit(usage);

  key_size = atoi(argv[1]);
  num_cells = atoi(argv[2]);
  num_scanners = atoi(argv[3]);
  intervals_per_scanner = atoi(argv[4]);

  cout << "key_size=" << key_size << ", num_cells=" << num_cells << ", num_scanners="
       << num_scanners << ", intervals_per_scanner=" << intervals_per_scanner << endl;

  Config::init(0, 0);

  ReactorFactory::initialize(2);
  String format_str = (String)"%0" + format("%lulu", (unsigned long)key_size);

  try {
    hypertable_client_ptr = new Hypertable::Client(System::locate_install_dir(argv[0]), "./hypertable.cfg");
    namespace_ptr = hypertable_client_ptr->open_namespace("/");
    table_ptr = namespace_ptr->open_table("LoadTest");
    // Do asynchronous scan
    FuturePtr future_ptr = new Future(5);
    vector<TableScannerAsyncPtr> scanners;
    TableScannerAsyncPtr scanner;
    ResultPtr result;
    Cells cells;
    String start_str;
    size_t start;

    for (ii=0; ii < num_scanners; ++ii) {
      ssbuilder.clear();
      for (jj=0; jj < intervals_per_scanner; jj++) {
        start = (num_cells*ii/num_scanners) + (num_cells*jj)/(num_scanners*intervals_per_scanner);
        start_str = format(format_str.c_str(), start);
        ssbuilder.add_row_interval(start_str.c_str(),true, "", false);
      }
      scan_spec = ssbuilder.get();
      cout << "Creating scanner with start_row=" << start_str << endl;
      scanner = table_ptr->create_scanner_async(future_ptr.get(), scan_spec);
      scanners.push_back(scanner);
    }

    ii = 0;
    while(future_ptr->get(result) && ii < num_cells) {
      if (result->is_error()) {
        int error;
        String error_msg;
        result->get_error(error, error_msg);
        HT_THROW(error, error_msg);
      }
      result->get_cells(cells);
      for (jj=0; jj < cells.size(); ++jj, ++ii) {
        if (ii >= num_cells) {
          // Dont deadlock in async scanner destruction
          future_ptr->cancel();
          break;
        }
      }
    }
  }
  catch (Hypertable::Exception &e) {
    cerr << e << endl;
    exit(1);
  }

  if (ii != num_cells) {
    cout << "Expected " << num_cells << " cells, received " << ii << endl;
    exit(1);
  }
  cout << "Test passed"<< endl;

  exit(0);
}
