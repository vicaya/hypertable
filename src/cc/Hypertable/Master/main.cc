/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <string>

#include "Common/System.h"
#include "Common/Usage.h"
#include "Common/WorkQueue.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionHandlerFactory.h"

#include "ConnectionHandler.h"
#include "CreateDirectoryLayout.h"
#include "Master.h"

using namespace hypertable;
using namespace std;


namespace {
  const char *usage[] = {
    "usage: Hypertable.Master [OPTIONS]",
    "",
    "OPTIONS:",
    "  --config=<file>  Read configuration from <file>.  The default config file is",
    "                   \"conf/hypertable.cfg\" relative to the toplevel install directory",
    "  --initialize     Create directories and files required for Master operation in",
    "                   hyperspace",
    "  --help           Display this help text and exit",
    "  --verbose,-v     Generate verbose output",
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
  HandlerFactory(Comm *comm, WorkQueue *workQueue, Master *master) : mComm(comm), mWorkQueue(workQueue), mMaster(master) { return; }
  CallbackHandler *newInstance() {
    return new ConnectionHandler(mComm, mWorkQueue, mMaster);
  }
private:
  Comm      *mComm;
  WorkQueue *mWorkQueue;
  Master    *mMaster;
};



/**
 * 
 */
int main(int argc, char **argv) {
  string configFile = "";
  Properties *props = 0;
  bool verbose = false;
  bool initialize = false;
  Master *master = 0;
  int port, reactorCount, workerCount;
  Comm *comm;
  WorkQueue *workQueue = 0;

  System::Initialize(argv[0]);
  
  if (argc > 1) {
    for (int i=1; i<argc; i++) {
      if (!strncmp(argv[i], "--config=", 9))
	configFile = &argv[i][9];
      else if (!strcmp(argv[i], "--initialize"))
	initialize = true;
      else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
	verbose = true;
      else
	Usage::DumpAndExit(usage);
    }
  }

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  props = new Properties(configFile);
  if (verbose)
    props->setProperty("verbose", "true");

  port         = props->getPropertyInt("Hypertable.Master.port", DEFAULT_PORT);
  reactorCount = props->getPropertyInt("Hypertable.Master.reactors", System::GetProcessorCount());
  workerCount  = props->getPropertyInt("Hypertable.Master.workers", DEFAULT_WORKERS);

  ReactorFactory::Initialize(reactorCount);

  comm = new Comm(0);

  if (verbose) {
    cout << "CPU count = " << System::GetProcessorCount() << endl;
    cout << "Hypertable.Master.port=" << port << endl;
    cout << "Hypertable.Master.workers=" << workerCount << endl;
    cout << "Hypertable.Master.reactors=" << reactorCount << endl;
  }

  if (initialize) {
    if (!CreateDirectoryLayout(comm, props))
      return 1;
    return 0;
  }

  master = new Master(comm, props);
  workQueue = new WorkQueue(workerCount);
  comm->Listen(port, new HandlerFactory(comm, workQueue, master));
  workQueue->Join();

  delete workQueue;
  delete master;
  delete props;
  delete comm;
  return 0;
}
