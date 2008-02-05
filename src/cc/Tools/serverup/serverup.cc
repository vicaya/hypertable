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

extern "C" {
#include <netdb.h>
}

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorFactory.h"

#include "DfsBroker/Lib/Client.h"

#include "Hyperspace/Session.h"
#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/RangeServerClient.h"

using namespace Hypertable;
using namespace std;

namespace {

  const char *usage[] = {
    "usage: serverup [options] <servername>",
    "",
    "  options:",
    "    --config=<file>  Read configuration from <file>.  The default config file",
    "                     is \"conf/hypertable.cfg\" relative to the toplevel",
    "                     install directory",
    "    --host=<name>    Connect to the server running on host <name>",
    "    --port=<n>       Connect to the server running on port <n>",
    "    --help           Display this help text and exit",
    "    --verbose,-v     Display 'true' if up, 'false' otherwise",
    "",
    "  This program check checks to see if the server, specified by <servername>,",
    "  is up.  It does this by determining the host and port that the server is",
    "  listening on from the command line options, or if not supplied, from the",
    "  the config file.  It then establishes a connection with the server and,",
    "  sends a STATUS request.  If the response from the STATUS request is OK,",
    "  then the program terminates with an exit status of 0.  If a failure occurs,",
    "  or a non-OK error code is returned from the STATUS request, the program",
    "  terminates with an exit status of 1.  <servername> may be one of the",
    "  following values:",
    "",
    "  dfsbroker",
    "  hyperspace",
    "  master",
    "  rangeserver",
    "",
    (const char *)0
  };
  
}


/**
 *
 */
int main(int argc, char **argv) {
  string configFile = "";
  string serverName = "";
  string hostName = "";
  int port = 0;
  PropertiesPtr props_ptr;
  struct sockaddr_in addr;
  const char *hostProperty = 0;
  const char *portProperty = 0;
  const char *portStr = 0;
  Comm *comm = 0;
  ConnectionManagerPtr connManagerPtr;
  DfsBroker::Client *client;
  Hyperspace::SessionPtr hyperspacePtr;
  RangeServerClient *rangeServer;
  ApplicationQueuePtr appQueuePtr;
  int error;
  bool verbose = false;

  System::initialize(argv[0]);
  ReactorFactory::initialize((uint16_t)System::get_processor_count());

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      configFile = &argv[i][9];
    else if (!strncmp(argv[i], "--host=", 7))
      hostName = &argv[i][7];
    else if (!strncmp(argv[i], "--port=", 7))
      portStr = &argv[i][7];
    else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
      verbose = true;
    else if (argv[i][0] == '-' || serverName != "")
      Usage::dump_and_exit(usage);
    else
      serverName = argv[i];
  }

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  props_ptr = new Properties(configFile);

  if (serverName == "dfsbroker") {
    hostProperty = "DfsBroker.Host";
    portProperty = "DfsBroker.Port";
  }
  else if (serverName == "hyperspace") {
    hostProperty = "Hyperspace.Master.Host";
    portProperty = "Hyperspace.Master.Port";
  }
  else if (serverName == "master") {
    hostProperty = "Hypertable.Master.Host";
    portProperty = "Hypertable.Master.Port";
  }
  else if (serverName == "rangeserver") {
    if (hostName == "")
      hostName = "localhost";
    portProperty = "Hypertable.RangeServer.Port";
  }
  else
    Usage::dump_and_exit(usage);

  {
    if (hostName == "")
      hostName = props_ptr->get(hostProperty, "localhost");
    else if (serverName != "rangeserver")
      props_ptr->set(hostProperty, hostName.c_str());

    if (portStr != 0)
      props_ptr->set(portProperty, portStr);

    port = props_ptr->get_int(portProperty, 0);
    if (port == 0 || port < 1024 || port >= 65536) {
      HT_ERRORF("%s not specified or out of range : %d", portProperty, port);
      return 1;
    }

    if (!InetAddr::initialize(&addr, hostName.c_str(), (uint16_t)port))
      goto abort;
  }

  props_ptr->set("silent", "true");

  comm = new Comm();
  connManagerPtr = new ConnectionManager(comm);
  connManagerPtr->set_quiet_mode(true);

  if (serverName == "dfsbroker") {
    client = new DfsBroker::Client(connManagerPtr, addr, 30);
    if (!client->wait_for_connection(2))
      goto abort;
    if ((error = client->status()) != Error::OK)
      goto abort;
  }
  else if (serverName == "hyperspace") {
    hyperspacePtr = new Hyperspace::Session(connManagerPtr->get_comm(), props_ptr, 0);
    if (!hyperspacePtr->wait_for_connection(2))
      goto abort;
    if ((error = hyperspacePtr->status()) != Error::OK)
      goto abort;
  }
  else if (serverName == "master") {
    MasterClientPtr  masterPtr;

    hyperspacePtr = new Hyperspace::Session(connManagerPtr->get_comm(), props_ptr, 0);

    if (!hyperspacePtr->wait_for_connection(2))
      goto abort;

    appQueuePtr = new ApplicationQueue(1);
    masterPtr = new MasterClient(connManagerPtr, hyperspacePtr, 30, appQueuePtr);
    masterPtr->set_verbose_flag(false);
    if (masterPtr->initiate_connection(0) != Error::OK)
      goto abort;

    if (!masterPtr->wait_for_connection(2))
      goto abort;

    if ((error = masterPtr->status()) != Error::OK)
      goto abort;
  }
  else if (serverName == "rangeserver") {
    connManagerPtr->add(addr, 30, "Range Server");
    if (!connManagerPtr->wait_for_connection(addr, 2))
      goto abort;
    rangeServer = new RangeServerClient(comm, 30);
    if ((error = rangeServer->status(addr)) != Error::OK)
      goto abort;
  }

  if (verbose)
    cout << "true" << endl;
  return 0;

 abort:
  if (verbose)
    cout << "false" << endl;
  return 1;
}
