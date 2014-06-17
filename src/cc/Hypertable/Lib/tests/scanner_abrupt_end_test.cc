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
#include "Hypertable/Lib/KeySpec.h"

#include "AsyncComm/ReactorFactory.h"

using namespace Hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: ScannerCancelTest numCells",
    "This test creates a scanner on the LoadTest table and destroys the scanner before it ",
    "completes, once it reads numCells cells",
    "",
    0
  };

}


int main(int argc, char **argv) {
  ClientPtr hypertable_client_ptr;
  NamespacePtr namespace_ptr;
  TablePtr table_ptr;
  TableScannerPtr scanner;
  ScanSpecBuilder scan_spec;
  Cell cell;
  size_t num_cells=0;
  size_t ii=0;

  if ((argc > 1 && (!strcmp(argv[1], "-?") || !strcmp(argv[1], "--help"))) || argc != 2)
    Usage::dump_and_exit(usage);

  num_cells = atoi(argv[1]);
  Config::init(0, 0);

  ReactorFactory::initialize(2);

  try {
    hypertable_client_ptr = new Hypertable::Client(System::locate_install_dir(argv[0]), "./hypertable.cfg");
    namespace_ptr = hypertable_client_ptr->open_namespace("/");
    table_ptr = namespace_ptr->open_table("RandomTest");
    scanner = table_ptr->create_scanner(scan_spec.get());
    while(scanner->next(cell) && ii < num_cells) {
      ++ii;
    }
    scanner = 0;
    if (ii != num_cells) {
      cout << "Expected " << num_cells << " cells, received " << ii << endl;
      exit(1);
    }
  }
  catch (Hypertable::Exception &e) {
    cerr << e << endl;
    _exit(1);
  }

  cout << "Test passed"<< endl;

  exit(0);
}
