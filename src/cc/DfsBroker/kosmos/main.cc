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
  string configFile = "";
  string pidFile = "";
  Hypertable::PropertiesPtr propsPtr;
  bool verbose = false;
  int port, reactorCount, workerCount;
  Comm *comm;
  BrokerPtr broker_ptr;
  ApplicationQueuePtr app_queue_ptr;
  struct sockaddr_in listen_addr;
  int error;

  System::initialize(argv[0]);
  
  if (argc > 1) {
    for (int i=1; i<argc; i++) {
      if (!strncmp(argv[i], "--config=", 9))
	configFile = &argv[i][9];
      if (!strncmp(argv[i], "--pidfile=", 10))
	pidFile = &argv[i][10];
      else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
	verbose = true;
      else
	Usage::dump_and_exit(usage);
    }
  }

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  if (!FileUtils::exists(configFile.c_str())) {
    cerr << "Error: Unable to open config file '" << configFile << "'" << endl;
    exit(0);
  }

  propsPtr = new Hypertable::Properties(configFile);
  if (verbose)
    propsPtr->set("Hypertable.Verbose", "true");

  port         = propsPtr->get_int("DfsBroker.Port",     DEFAULT_PORT);
  workerCount  = propsPtr->get_int("DfsBroker.Workers",  DEFAULT_WORKERS);
  reactorCount = propsPtr->get_int("Kfs.Reactors", System::get_processor_count());

  ReactorFactory::initialize(reactorCount);

  comm = new Comm();

  if (verbose) {
    cout << "CPU count = " << System::get_processor_count() << endl;
    cout << "DfsBroker.Port=" << port << endl;
    cout << "DfsBroker.Workers=" << workerCount << endl;
    cout << "Kfs.reactors=" << reactorCount << endl;
  }

  InetAddr::initialize(&listen_addr, INADDR_ANY, port);

  broker_ptr = new KosmosBroker(propsPtr);
  app_queue_ptr = new ApplicationQueue(workerCount);
  ConnectionHandlerFactoryPtr chf_ptr(new DfsBroker::ConnectionHandlerFactory(comm, app_queue_ptr, broker_ptr));
  if ((error = comm->listen(listen_addr, chf_ptr)) != Error::OK) {
    std::string addrStr;
    HT_ERRORF("Problem listening for connections on %s - %s", InetAddr::string_format(addrStr, listen_addr), Error::get_text(error));
    return 1;
  }

  if (pidFile != "") {
    fstream filestr (pidFile.c_str(), fstream::out);
    filestr << getpid() << endl;
    filestr.close();
  }

  app_queue_ptr->join();

  delete comm;
  return 0;
}
