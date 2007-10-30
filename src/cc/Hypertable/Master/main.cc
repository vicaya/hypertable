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

#include "Common/InetAddr.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionHandlerFactory.h"

#include "ConnectionHandler.h"
#include "Master.h"

using namespace hypertable;
using namespace std;


namespace {
  const char *usage[] = {
    "usage: Hypertable.Master [OPTIONS]",
    "",
    "OPTIONS:",
    "  --config=<file>   Read configuration from <file>.  The default config file is",
    "                    \"conf/hypertable.cfg\" relative to the toplevel install directory",
    "  --pidfile=<fname> Write the process ID to <fname> upon successful startup",
    "  --help            Display this help text and exit",
    "  --verbose,-v      Generate verbose output",
    ""
    "This program is the Hypertable master server.",
    (const char *)0
  };
  const int DEFAULT_PORT              = 38548;
  const int DEFAULT_WORKERS           = 20;
}



/**
 *
 */
class HandlerFactory : public ConnectionHandlerFactory {
public:
  HandlerFactory(Comm *comm, ApplicationQueue *appQueue, Master *master) : mComm(comm), mAppQueue(appQueue), mMaster(master) { return; }
  DispatchHandler *newInstance() {
    return new ConnectionHandler(mComm, mAppQueue, mMaster);
  }
private:
  Comm              *mComm;
  ApplicationQueue  *mAppQueue;
  Master            *mMaster;
};



/**
 * 
 */
int main(int argc, char **argv) {
  string configFile = "";
  string pidFile = "";
  PropertiesPtr propsPtr;
  bool verbose = false;
  Master *master = 0;
  int port, reactorCount, workerCount;
  Comm *comm;
  ApplicationQueue *appQueue = 0;
  ConnectionManager *connManager;
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

  propsPtr.reset( new Properties(configFile) );
  if (verbose)
    propsPtr->setProperty("verbose", "true");

  port         = propsPtr->getPropertyInt("Hypertable.Master.port", DEFAULT_PORT);
  reactorCount = propsPtr->getPropertyInt("Hypertable.Master.reactors", System::GetProcessorCount());
  workerCount  = propsPtr->getPropertyInt("Hypertable.Master.workers", DEFAULT_WORKERS);

  ReactorFactory::Initialize(reactorCount);

  comm = new Comm();
  connManager = new ConnectionManager(comm);

  if (verbose) {
    cout << "CPU count = " << System::GetProcessorCount() << endl;
    cout << "Hypertable.Master.port=" << port << endl;
    cout << "Hypertable.Master.workers=" << workerCount << endl;
    cout << "Hypertable.Master.reactors=" << reactorCount << endl;
  }

  appQueue = new ApplicationQueue(workerCount);
  master = new Master(connManager, propsPtr, appQueue);

  InetAddr::Initialize(&listenAddr, INADDR_ANY, port);
  comm->Listen(listenAddr, new HandlerFactory(comm, appQueue, master));

  if (pidFile != "") {
    fstream filestr (pidFile.c_str(), fstream::out);
    filestr << getpid() << endl;
    filestr.close();
  }

  poll(0, 0, -1);

  appQueue->Shutdown();

  delete appQueue;
  delete master;
  delete connManager;
  delete comm;
  return 0;
}
