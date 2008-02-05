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
#include <iostream>
#include <fstream>
#include <string>

extern "C" {
#include <poll.h>
#include <sys/types.h>
#include <unistd.h>
}

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/InetAddr.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"

#include "DfsBroker/Lib/ConnectionHandlerFactory.h"

#include "LocalBroker.h"

using namespace Hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: localBroker [OPTIONS]",
    "",
    "OPTIONS:",
    "  --config=<file>       Read configuration from <file>.  The default config",
    "                        file is \"conf/hypertable.cfg\" relative to the toplevel",
    "                        install directory",
    "  --listen-port=<port>  Listen for connections on port <port>",
    "  --pidfile=<fname>     Write the process ID to <fname> upon successful startup",
    "  --help                Display this help text and exit",
    "  --verbose,-v          Generate verbose output",
    "",
    "This program is the local DFS broker server.",
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
  PropertiesPtr props_ptr;
  bool verbose = false;
  int reactorCount, workerCount;
  uint16_t port = 0;
  Comm *comm;
  BrokerPtr brokerPtr;
  ApplicationQueuePtr appQueuePtr;
  struct sockaddr_in listenAddr;
  int error;

  System::initialize(argv[0]);
  
  if (argc > 1) {
    for (int i=1; i<argc; i++) {
      if (!strncmp(argv[i], "--config=", 9))
	configFile = &argv[i][9];
      else if (!strncmp(argv[i], "--listen-port=", 14))
	port = (uint16_t)atoi(&argv[i][14]);
      else if (!strncmp(argv[i], "--pidfile=", 10))
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

  props_ptr = new Properties(configFile);
  if (verbose)
    props_ptr->set("verbose", "true");

  if (port == 0)
    port       = props_ptr->get_int("DfsBroker.Local.Port",     DEFAULT_PORT);
  reactorCount = props_ptr->get_int("DfsBroker.Local.Reactors", System::get_processor_count());
  workerCount  = props_ptr->get_int("DfsBroker.Local.Workers",  DEFAULT_WORKERS);

  ReactorFactory::initialize(reactorCount);

  comm = new Comm();

  if (verbose) {
    cout << "CPU count = " << System::get_processor_count() << endl;
    cout << "DfsBroker.Local.Port=" << port << endl;
    cout << "DfsBroker.Local.Reactors=" << reactorCount << endl;
    cout << "DfsBroker.Local.Workers=" << workerCount << endl;
  }

  InetAddr::initialize(&listenAddr, INADDR_ANY, port);

  brokerPtr = new LocalBroker(props_ptr);
  appQueuePtr = new ApplicationQueue(workerCount);
  ConnectionHandlerFactoryPtr chfPtr(new DfsBroker::ConnectionHandlerFactory(comm, appQueuePtr, brokerPtr));
  if ((error = comm->listen(listenAddr, chfPtr)) != Error::OK) {
    std::string addrStr;
    HT_ERRORF("Problem listening for connections on %s - %s", InetAddr::string_format(addrStr, listenAddr), Error::get_text(error));
    return 1;
  }

  if (pidFile != "") {
    fstream filestr (pidFile.c_str(), fstream::out);
    filestr << getpid() << endl;
    filestr.close();
  }

  appQueuePtr->join();

  delete comm;
  return 0;
}
