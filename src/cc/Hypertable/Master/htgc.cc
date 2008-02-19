/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include <iostream>
#include "Common/Thread.h"
#include "Common/System.h"
#include "Common/Logger.h"
#include "Common/Error.h"
#include "Hypertable/Lib/Client.h"
#include "DfsBroker/Lib/Client.h"
#include "MasterGc.h"

using namespace Hypertable;
using namespace std;

namespace {

void 
do_tfgc(const char *config, bool debug, bool dryrun) {
  if (debug)
    Logger::set_level(log4cpp::Priority::DEBUG);

  try {
    PropertiesPtr props = new Properties(config);
    Comm *comm = new Comm();
    ConnectionManagerPtr conn_mgr = new ConnectionManager(comm);
    DfsBroker::Client *fs = new DfsBroker::Client(conn_mgr, props);
    ClientPtr ht_ptr = new Hypertable::Client("testgc", config);
    TablePtr table_ptr;

    int ret = ht_ptr->open_table("METADATA", table_ptr);

    if (ret != Error::OK) {
      cerr << "Error opening METADATA" << endl;
      exit(1);
    }
    master_gc_once(table_ptr, fs, dryrun);
  }
  catch (exception &e) {
    cerr << "Error: "<< e.what() << endl;
    exit(1);
  }
}

} // local namespace

int 
main(int ac, char *av[]) {
  char **it = av + 1, **arg_end = av + ac;
  bool debug = false, dryrun = false;
  char *config = "conf/hypertable.cfg";
  System::initialize(av[0]);

  for (; it < arg_end; ++it) {
    if (!strcmp("-d", *it))
      debug = true;
    else if (!strcmp("-n", *it))
      dryrun = true;
    else if (!strcmp("-f", *it))
      config = *++it;
  }
  ReactorFactory::initialize(4);
  do_tfgc(config, debug, dryrun);

  return 0;
}
