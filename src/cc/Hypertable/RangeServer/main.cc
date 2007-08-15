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

#include <string>

extern "C" {
#include <poll.h>
}

#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionHandlerFactory.h"

#include "ConnectionHandler.h"
#include "Global.h"
#include "RangeServer.h"

using namespace hypertable;
using namespace std;


namespace {
  const char *usage[] = {
    "usage: Hypertable.RangeServer --metadata=<file> [OPTIONS]",
    "",
    "OPTIONS:",
    "  --config=<file>  Read configuration from <file>.  The default config file is",
    "                   \"conf/hypertable.cfg\" relative to the toplevel install directory",
    "  --help             Display this help text and exit",
    "  --verbose,-v       Generate verbose output",
    ""
    "This program is the Hypertable range server.",
    (const char *)0
  };
  const int DEFAULT_PORT    = 38549;
  const int DEFAULT_WORKERS = 20;
}


/**
 *
 */
class HandlerFactory : public ConnectionHandlerFactory {
public:
  HandlerFactory(Comm *comm, ApplicationQueue *appQueue, RangeServer *rangeServer) : mComm(comm), mAppQueue(appQueue), mRangeServer(rangeServer) { return; }
  DispatchHandler *newInstance() {
    return new ConnectionHandler(mComm, mAppQueue, mRangeServer);
  }
private:
  Comm        *mComm;
  ApplicationQueue   *mAppQueue;
  RangeServer *mRangeServer;
};



/**
 * 
 */
int main(int argc, char **argv) {
  string configFile = "";
  string metadataFile = "";
  int port, reactorCount, workerCount;
  Comm *comm = 0;
  Properties *props = 0;
  RangeServer *rangeServer = 0;

  System::Initialize(argv[0]);
  Global::verbose = false;
  
  if (argc > 1) {
    for (int i=1; i<argc; i++) {
      if (!strncmp(argv[i], "--config=", 9))
	configFile = &argv[i][9];
      else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
	Global::verbose = true;
      else if (!strncmp(argv[i], "--metadata=", 11)) {
	metadataFile = &argv[i][11];
      }
      else
	Usage::DumpAndExit(usage);
    }
  }

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  props = new Properties(configFile);
  if (Global::verbose)
    props->setProperty("verbose", "true");

  if (metadataFile == "") {
    LOG_ERROR("--metadata argument not specified.");
    exit(1);
  }
  props->setProperty("metadata", metadataFile.c_str());

  port         = props->getPropertyInt("Hypertable.RangeServer.port", DEFAULT_PORT);
  reactorCount = props->getPropertyInt("Hypertable.RangeServer.reactors", System::GetProcessorCount());
  workerCount  = props->getPropertyInt("Hypertable.RangeServer.workers", DEFAULT_WORKERS);

  ReactorFactory::Initialize(reactorCount);

  comm = new Comm();

  if (Global::verbose) {
    cout << "CPU count = " << System::GetProcessorCount() << endl;
    cout << "Hypertable.RangeServer.port=" << port << endl;
    cout << "Hypertable.RangeServer.workers=" << workerCount << endl;
    cout << "Hypertable.RangeServer.reactors=" << reactorCount << endl;
  }

  rangeServer = new RangeServer (comm, props);
  Global::appQueue = new ApplicationQueue(workerCount);
  comm->Listen(port, new HandlerFactory(comm, Global::appQueue, rangeServer));

  poll(0, 0, -1);

  Global::appQueue->Shutdown();

  delete Global::appQueue;
  delete rangeServer;
  delete props;
  delete comm;
  return 0;
}
