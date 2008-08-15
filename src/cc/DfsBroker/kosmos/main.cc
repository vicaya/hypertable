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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include <iostream>
#include <fstream>
#include <string>

extern "C" {
#include <poll.h>
#include <sys/types.h>
#include <unistd.h>
}

#include "Common/FileUtils.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"

#include "DfsBroker/Lib/ConnectionHandlerFactory.h"

#include "KosmosBroker.h"

using namespace Hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: kosmosBroker [OPTIONS]",
    "",
    "OPTIONS:",
    "  --config=<file>   Read configuration from <file>.  The default config file is",
    "                    \"conf/hypertable.cfg\" relative to the toplevel install directory",
    "  --pidfile=<fname> Write the process ID to <fname> upon successful startup",
    "  --help            Display this help text and exit",
    "  --verbose,-v      Generate verbose output",
    ""
    "This program is the local DFS broker.",
    (const char *)0
  };
  const int DEFAULT_PORT    = 38030;
  const int DEFAULT_WORKERS = 20;
}



/**
 *
 */
int main(int argc, char **argv) {
  string cfg_file = "";
  string pidfile = "";
  Hypertable::PropertiesPtr props;
  bool verbose = false;
  int port, reactor_count, worker_count;
  Comm *comm;
  BrokerPtr broker_ptr;
  ApplicationQueuePtr app_queue_ptr;
  struct sockaddr_in listen_addr;

  System::initialize(System::locate_install_dir(argv[0]));

  if (argc > 1) {
    for (int i=1; i<argc; i++) {
      if (!strncmp(argv[i], "--config=", 9))
        cfg_file = &argv[i][9];
      else if (!strncmp(argv[i], "--pidfile=", 10))
        pidfile = &argv[i][10];
      else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
        verbose = true;
      else
        Usage::dump_and_exit(usage);
    }
  }

  if (cfg_file == "")
    cfg_file = System::install_dir + "/conf/hypertable.cfg";

  if (!FileUtils::exists(cfg_file.c_str())) {
    cerr << "Error: Unable to open config file '" << cfg_file << "'" << endl;
    exit(0);
  }

  props = new Hypertable::Properties(cfg_file);
  if (verbose)
    props->set("Hypertable.Verbose", "true");

  port         = props->get_int("DfsBroker.Port",     DEFAULT_PORT);
  worker_count  = props->get_int("DfsBroker.Workers",  DEFAULT_WORKERS);
  reactor_count = props->get_int("Kfs.Reactors", System::get_processor_count());

  ReactorFactory::initialize(reactor_count);

  comm = Comm::instance();

  if (verbose) {
    cout << "CPU count = " << System::get_processor_count() << endl;
    cout << "DfsBroker.Port=" << port << endl;
    cout << "DfsBroker.Workers=" << worker_count << endl;
    cout << "Kfs.reactors=" << reactor_count << endl;
  }

  InetAddr::initialize(&listen_addr, INADDR_ANY, port);

  broker_ptr = new KosmosBroker(props);
  app_queue_ptr = new ApplicationQueue(worker_count);
  ConnectionHandlerFactoryPtr chf_ptr(new DfsBroker::ConnectionHandlerFactory(comm, app_queue_ptr, broker_ptr));
  comm->listen(listen_addr, chf_ptr);

  if (pidfile != "") {
    fstream filestr (pidfile.c_str(), fstream::out);
    filestr << getpid() << endl;
    filestr.close();
  }

  app_queue_ptr->join();

  return 0;
}
