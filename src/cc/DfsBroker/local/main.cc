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

#include <iostream>
#include <fstream>
#include <string>

extern "C" {
#include <poll.h>
#include <sys/types.h>
#include <unistd.h>
}

#include "Common/FileUtils.h"
#include "Common/InetAddr.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"

#include "DfsBroker/Lib/ConnectionHandlerFactory.h"

#include "LocalBroker.h"

using namespace hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: localBroker [OPTIONS]",
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
  const int DEFAULT_PORT    = 38546;
  const int DEFAULT_WORKERS = 20;
}



/**
 * 
 */
int main(int argc, char **argv) {
  string configFile = "";
  string pidFile = "";
  PropertiesPtr propsPtr;
  bool verbose = false;
  int port, reactorCount, workerCount;
  Comm *comm;
  LocalBroker *broker = 0;
  ApplicationQueue *appQueue = 0;
  struct sockaddr_in listenAddr;

  System::Initialize(argv[0]);
  
  if (argc > 1) {
    for (int i=1; i<argc; i++) {
      if (!strncmp(argv[i], "--config=", 9))
	configFile = &argv[i][9];
      if (!strncmp(argv[i], "--pidfile=", 10))
	pidFile = &argv[i][10];
      else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
	verbose = true;
      else
	Usage::DumpAndExit(usage);
    }
  }

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  if (!FileUtils::Exists(configFile.c_str())) {
    cerr << "Error: Unable to open config file '" << configFile << "'" << endl;
    exit(0);
  }

  propsPtr.reset( new Properties(configFile) );
  if (verbose)
    propsPtr->setProperty("verbose", "true");

  port         = propsPtr->getPropertyInt("DfsBroker.local.port",     DEFAULT_PORT);
  reactorCount = propsPtr->getPropertyInt("DfsBroker.local.reactors", System::GetProcessorCount());
  workerCount  = propsPtr->getPropertyInt("DfsBroker.local.workers",  DEFAULT_WORKERS);

  ReactorFactory::Initialize(reactorCount);

  comm = new Comm();

  if (verbose) {
    cout << "CPU count = " << System::GetProcessorCount() << endl;
    cout << "DfsBroker.local.port=" << port << endl;
    cout << "DfsBroker.local.reactors=" << reactorCount << endl;
    cout << "DfsBroker.local.workers=" << workerCount << endl;
  }

  InetAddr::Initialize(&listenAddr, INADDR_ANY, port);

  broker = new LocalBroker(propsPtr);
  appQueue = new ApplicationQueue(workerCount);
  comm->Listen(listenAddr, new DfsBroker::ConnectionHandlerFactory(comm, appQueue, broker));

  if (pidFile != "") {
    fstream filestr (pidFile.c_str(), fstream::out);
    filestr << getpid() << endl;
    filestr.close();
  }

  poll(0, 0, -1);

  appQueue->Shutdown();

  delete appQueue;
  delete broker;
  delete comm;
  return 0;
}
