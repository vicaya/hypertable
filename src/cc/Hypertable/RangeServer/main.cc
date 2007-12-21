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
#include <cstring>
#include <iostream>
#include <fstream>
#include <string>

extern "C" {
#include <poll.h>
#include <sys/types.h>
#include <unistd.h>
}

#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"

#include "ConnectionHandler.h"
#include "Global.h"
#include "HyperspaceSessionHandler.h"
#include "RangeServer.h"

using namespace Hypertable;
using namespace std;


namespace {
  const char *usage[] = {
    "usage: Hypertable.range_server [OPTIONS]",
    "",
    "OPTIONS:",
    "  --config=<file>      Read configuration from <file>.  The default config file is",
    "                       \"conf/hypertable.cfg\" relative to the toplevel install directory",
    "  --help               Display this help text and exit",
    "  --log-broker=<addr>  Use the DFS broker located at <addr> for the commit log.",
    "                       <addr> is of the format <host>:<port>",
    "  --pidfile=<fname>    Write the process ID to <fname> upon successful startup",
    "  --verbose,-v         Generate verbose output",
    ""
    "This program is the Hypertable range server.",
    (const char *)0
  };
  const int DEFAULT_WORKERS = 20;
}


/**
 * 
 */
int main(int argc, char **argv) {
  ConnectionManagerPtr conn_manager_ptr;
  ApplicationQueuePtr app_queue_ptr;
  Hyperspace::SessionPtr hyperspace_ptr;
  CommPtr comm_ptr;
  string configFile = "";
  string pidFile = "";
  int reactorCount;
  char *logBroker = 0;
  PropertiesPtr propsPtr;
  int workerCount;

  System::initialize(argv[0]);
  Global::verbose = false;
  
  if (argc > 1) {
    for (int i=1; i<argc; i++) {
      if (!strncmp(argv[i], "--config=", 9))
	configFile = &argv[i][9];
      else if (!strncmp(argv[i], "--log-broker=", 13))
	logBroker = &argv[i][13];
      else if (!strncmp(argv[i], "--pidfile=", 10))
	pidFile = &argv[i][10];
      else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
	Global::verbose = true;
      else
	Usage::dump_and_exit(usage);
    }
  }

  /**
   * Seed random number generator with PID
   */
  srand((unsigned)getpid());

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  propsPtr.reset( new Properties(configFile) );
  if (Global::verbose)
    propsPtr->setProperty("verbose", "true");

  if (logBroker != 0) {
    char *portStr = strchr(logBroker, ':');
    if (portStr == 0) {
      LOG_ERROR("Invalid address format for --log-broker, must be <host>:<port>");
      exit(1);
    }
    *portStr++ = 0;
    propsPtr->setProperty("Hypertable.RangeServer.CommitLog.DfsBroker.host", logBroker);
    propsPtr->setProperty("Hypertable.RangeServer.CommitLog.DfsBroker.port", portStr);
  }

  reactorCount = propsPtr->getPropertyInt("Hypertable.range_server.reactors", System::get_processor_count());
  ReactorFactory::initialize(reactorCount);
  comm_ptr = new Comm();

  workerCount = propsPtr->getPropertyInt("Hypertable.RangeServer.workers", DEFAULT_WORKERS);

  conn_manager_ptr = new ConnectionManager(comm_ptr.get());
  app_queue_ptr = new ApplicationQueue(workerCount);

  /**
   * Connect to Hyperspace
   */
  hyperspace_ptr = new Hyperspace::Session(comm_ptr.get(), propsPtr, new HyperspaceSessionHandler());
  if (!hyperspace_ptr->wait_for_connection(30)) {
    LOG_ERROR("Unable to connect to hyperspace, exiting...");
    exit(1);
  }

  /**
   * Open METADATA table
   */
  Global::metadata_table_ptr = new Table(conn_manager_ptr, hyperspace_ptr, "METADATA");

  Global::range_metadata_max_bytes = propsPtr->getPropertyInt64("Hypertable.RangeServer.Range.MetadataMaxBytes", 0LL);

  if (Global::verbose) {
    cout << "CPU count = " << System::get_processor_count() << endl;
    cout << "Hypertable.range_server.reactors=" << reactorCount << endl;
    if (Global::range_metadata_max_bytes != 0)
      cout << "Hypertable.RangeServer.Range.MetadataMaxBytes=" << Global::range_metadata_max_bytes << endl;
  }

  RangeServerPtr range_server_ptr = new RangeServer(propsPtr, conn_manager_ptr, app_queue_ptr, hyperspace_ptr);

  if (pidFile != "") {
    fstream filestr (pidFile.c_str(), fstream::out);
    filestr << getpid() << endl;
    filestr.close();
  }

  app_queue_ptr->join();

  Global::metadata_table_ptr = 0;

  ReactorFactory::destroy();

  LOG_ERROR("Exiting RangeServer.");

  return 0;
}
