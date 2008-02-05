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

#include <cstdlib>
#include <iostream>

extern "C" {
#include <netdb.h>
}

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorFactory.h"

#include "DfsBroker/Lib/Client.h"

#include "Hypertable/Lib/CommitLogReader.h"

using namespace Hypertable;
using namespace std;

namespace {

  const char *usage[] = {
    "usage: dumplog [options] <log-dir>",
    "",
    "  options:",
    "    --config=<file>  Read configuration from <file>.  The default config file",
    "                     is \"conf/hypertable.cfg\" relative to the toplevel",
    "                     install directory",
    "    --help           Display this help text and exit",
    "    --verbose,-v     Display 'true' if up, 'false' otherwise",
    "",
    "  This program dumps the given log's metadata.",
    "",
    (const char *)0
  };
}


/**
 * 
 */
int main(int argc, char **argv) {
  string configFile = "";
  string log_dir;
  PropertiesPtr props_ptr;
  bool verbose = false;
  CommPtr comm_ptr;
  ConnectionManagerPtr conn_manager_ptr;
  DfsBroker::Client *dfs_client;
  CommitLogReader *log_reader;

  System::initialize(argv[0]);
  ReactorFactory::initialize((uint16_t)System::get_processor_count());

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      configFile = &argv[i][9];
    else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
      verbose = true;
    else if (log_dir == "")
      log_dir = argv[i];
    else
      Usage::dump_and_exit(usage);
  }

  if (log_dir == "")
      Usage::dump_and_exit(usage);
  
  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  props_ptr = new Properties(configFile);

  comm_ptr = new Comm();
  conn_manager_ptr = new ConnectionManager(comm_ptr.get());

  /**
   * Check for and connect to commit log DFS broker
   */
  {
    const char *logHost = props_ptr->get("Hypertable.RangeServer.CommitLog.DfsBroker.Host", 0);
    uint16_t logPort    = props_ptr->get_int("Hypertable.RangeServer.CommitLog.DfsBroker.Port", 0);
    struct sockaddr_in addr;
    if (logHost != 0) {
      InetAddr::initialize(&addr, logHost, logPort);
      dfs_client = new DfsBroker::Client(conn_manager_ptr, addr, 600);
    }
    else {
      dfs_client = new DfsBroker::Client(conn_manager_ptr, props_ptr);
    }

    if (!dfs_client->wait_for_connection(30)) {
      HT_ERROR("Unable to connect to DFS Broker, exiting...");
      exit(1);
    }
  }

  log_reader = new CommitLogReader(dfs_client, log_dir);

  log_reader->dump_log_metadata();

  delete log_reader;

  return 0;
}
