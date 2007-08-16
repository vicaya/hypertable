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
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ReactorFactory.h"

#include "HdfsClient/HdfsClient.h"
#include "Hyperspace/HyperspaceClient.h"
#include "Hypertable/Lib/MasterClient.h"

using namespace hypertable;
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
    "  hdfsbroker",
    "  hyperspace",
    "  master",
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
  PropertiesPtr propsPtr;
  struct sockaddr_in addr;
  const char *hostProperty = 0;
  const char *portProperty = 0;
  const char *portStr = 0;
  HdfsClient *hdfsClient;
  HyperspaceClient *hyperspaceClient;
  MasterClient *master;
  Comm *comm = 0;
  int error;

  System::Initialize(argv[0]);
  ReactorFactory::Initialize((uint16_t)System::GetProcessorCount());

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      configFile = &argv[i][9];
    else if (!strncmp(argv[i], "--host=", 7))
      hostName = &argv[i][7];
    else if (!strncmp(argv[i], "--port=", 7))
      portStr = &argv[i][7];
    else if (argv[i][0] == '-' || serverName != "")
      Usage::DumpAndExit(usage);
    else
      serverName = argv[i];
  }

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  propsPtr.reset( new Properties(configFile) );

  if (serverName == "hdfsbroker") {
    hostProperty = "HdfsBroker.host";
    portProperty = "HdfsBroker.port";
  }
  else if (serverName == "hyperspace") {
    hostProperty = "Hyperspace.host";
    portProperty = "Hyperspace.port";
  }
  else if (serverName == "master") {
    hostProperty = "Hypertable.Master.host";
    portProperty = "Hypertable.Master.port";
  }
  else
    Usage::DumpAndExit(usage);

  {
    if (hostName == "")
      hostName = propsPtr->getProperty(hostProperty, "localhost");
    else
      propsPtr->setProperty(hostProperty, hostName.c_str());

    if (portStr != 0)
      propsPtr->setProperty(portProperty, portStr);

    port = propsPtr->getPropertyInt(portProperty, 0);
    if (port == 0 || port < 1024 || port >= 65536) {
      LOG_VA_ERROR("%s not specified or out of range : %d", portProperty, port);
      exit(1);
    }

    memset(&addr, 0, sizeof(struct sockaddr_in));
    {
      struct hostent *he = gethostbyname(hostName.c_str());
      if (he == 0) {
	herror("gethostbyname()");
	exit(1);
      }
      memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
  }

  comm = new Comm();

  if (serverName == "hdfsbroker") {
    hdfsClient = new HdfsClient(comm, addr, 30, true);
    if (!hdfsClient->WaitForConnection(2))
      exit(1);
    if ((error = hdfsClient->Status()) != Error::OK)
      exit(1);
  }
  else if (serverName == "hyperspace") {
    hyperspaceClient = new HyperspaceClient(comm, addr, 2, true);
    if (!hyperspaceClient->WaitForConnection())
      exit(1);
    if ((error = hyperspaceClient->Status()) != Error::OK)
      exit(1);
  }
  else if (serverName == "master") {
    CommPtr commPtr(comm);
    master = new MasterClient(commPtr, propsPtr, true);
    if (!master->WaitForConnection(2))
      exit(1);
    if ((error = master->Status()) != Error::OK)
      exit(1);
  }

  return 0;
}
