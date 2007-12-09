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

#include "AsyncComm/Comm.h"

#include "ConnectionHandler.h"
#include "Global.h"
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
}


/**
 * 
 */
int main(int argc, char **argv) {
  string configFile = "";
  string pidFile = "";
  int reactorCount;
  char *logBroker = 0;
  Comm *comm = 0;
  PropertiesPtr propsPtr;

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
  comm = new Comm();

  Global::range_metadata_max_bytes = propsPtr->getPropertyInt64("Hypertable.RangeServer.Range.MetadataMaxBytes", 0LL);

  if (Global::verbose) {
    cout << "CPU count = " << System::get_processor_count() << endl;
    cout << "Hypertable.range_server.reactors=" << reactorCount << endl;
    if (Global::range_metadata_max_bytes != 0)
      cout << "Hypertable.RangeServer.Range.MetadataMaxBytes=" << Global::range_metadata_max_bytes << endl;
  }

  RangeServerPtr range_server_ptr = new RangeServer(comm, propsPtr);

  if (pidFile != "") {
    fstream filestr (pidFile.c_str(), fstream::out);
    filestr << getpid() << endl;
    filestr.close();
  }

  ApplicationQueuePtr app_queue_ptr = range_server_ptr->get_application_queue_ptr();

  app_queue_ptr->join();

  LOG_ERROR("Exiting RangeServer.");

  delete comm;
  return 0;
}
