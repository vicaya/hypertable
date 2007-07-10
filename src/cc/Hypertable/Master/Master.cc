/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include <string>

#include <boost/shared_array.hpp>

extern "C" {
#include <string.h>
}

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/StringExt.h"
#include "Common/System.h"
#include "Common/Usage.h"
#include "Common/WorkQueue.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionHandlerFactory.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorFactory.h"

#include "ConnectionHandler.h"
#include "CreateDirectoryLayout.h"
#include "Global.h"
#include "MasterProtocol.h"

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
  const int DEFAULT_RANGESERVER_PORT  = 38549;
  const int DEFAULT_WORKERS           = 20;

  bool LoadRangeServerVector(const char *rangeServers);
}



/**
 *
 */
class HandlerFactory : public ConnectionHandlerFactory {
public:
  CallbackHandler *newInstance() {
    return new ConnectionHandler();
  }
};



/**
 * 
 */
int main(int argc, char **argv) {
  string configFile = "";
  bool initialize = false;
  int port, reactorCount, workerCount;
  int exitValue = 1;
  const char *rangeServers;

  System::Initialize(argv[0]);
  
  if (argc > 1) {
    for (int i=1; i<argc; i++) {
      if (!strncmp(argv[i], "--config=", 9))
	configFile = &argv[i][9];
      else if (!strcmp(argv[i], "--initialize"))
	initialize = true;
      else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
	Global::verbose = true;
      else
	Usage::DumpAndExit(usage);
    }
  }

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  Global::props = new Properties(configFile);

  port = Global::props->getPropertyInt("Hypertable.Master.port", DEFAULT_PORT);
  workerCount = Global::props->getPropertyInt("Hypertable.Master.workers", DEFAULT_WORKERS);
  reactorCount = Global::props->getPropertyInt("Hypertable.Master.reactors", System::GetProcessorCount());
  rangeServers = Global::props->getProperty("Hypertable.Master.rangeServers", (const char *)0);

  // process range servers ...
  if (!LoadRangeServerVector(rangeServers))
    goto done;

  if (Global::verbose) {
    cout << "CPU count = " << System::GetProcessorCount() << endl;
    cout << "Hypertable.Master.port=" << port << endl;
    cout << "Hypertable.Master.workers=" << workerCount << endl;
    cout << "Hypertable.Master.reactors=" << reactorCount << endl;
  }

  ReactorFactory::Initialize(reactorCount);

  Global::comm = new Comm(0);
  Global::protocol = new MasterProtocol();
  Global::hyperspace = new HyperspaceClient(Global::comm, Global::props);

  if (!Global::hyperspace->WaitForConnection())
    goto done;

  /**
   * Create HDFS Client connection
   */
  {
    struct sockaddr_in addr;
    const char *host = Global::props->getProperty("HdfsBroker.Server.host", 0);
    if (host == 0) {
      cerr << "HdfsBroker.Server.host property not specified" << endl;
      goto done;
    }

    int port = Global::props->getPropertyInt("HdfsBroker.Server.port", 0);
    if (port == 0) {
      cerr << "HdfsBroker.Server.port property not specified" << endl;
      goto done;
    }

    if (Global::verbose) {
      cout << "HdfsBroker.host=" << host << endl;
      cout << "HdfsBroker.port=" << port << endl;
    }

    memset(&addr, 0, sizeof(struct sockaddr_in));
    {
      struct hostent *he = gethostbyname(host);
      if (he == 0) {
	herror("gethostbyname()");
	exit(1);
      }
      memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    Global::hdfsClient = new HdfsClient(Global::comm, addr, 20);
    if (!Global::hdfsClient->WaitForConnection(30))
      goto done;
  }

  if (initialize) {
    Global::workQueue = 0;
    CreateDirectoryLayout();
  }
  else {

    /* Read Last Table ID */
    {
      DynamicBuffer valueBuf(0);
      int error = Global::hyperspace->AttrGet("/hypertable/meta", "last_table_id", valueBuf);
      if (error != Error::OK)
	return error;

      if (valueBuf.fill() == 0) {
	cerr << "Error:  empty value for attribute 'last_table_id' of file '/hypertable/meta'" << endl;
	goto done;
      }
      else {
	int ival = strtol((const char *)valueBuf.buf, 0, 0);
	if (ival == 0 && errno == EINVAL) {
	  cerr << "Error: problem converting attribute 'last_table_id' (" << (const char *)valueBuf.buf << ") to an integer" << endl;
	  goto done;
	}
	atomic_set(&Global::lastTableId, ival);
	if (Global::verbose)
	  cout << "Last Table ID: " << ival << endl;
      }
    }

    Global::workQueue = new WorkQueue(workerCount);
    Global::comm->Listen(port, new HandlerFactory());
    Global::workQueue->Join();
  }

  exitValue = 0;

 done:
  delete Global::props;
  delete Global::protocol;
  delete Global::workQueue;
  delete Global::hyperspace;
  delete Global::comm;
  return exitValue;
}

namespace {

  bool LoadRangeServerVector(const char *rangeServers) {
    boost::shared_array<char> data( new char [ strlen(rangeServers) + 1 ] );
    char *base = data.get();
    char *ptr, *last;
    boost::shared_ptr<RangeServerInfoT> rsInfoPtr;
    int defaultPort;
    uint16_t port;
    struct sockaddr_in addr;
    std::string waitMessage;
    
    defaultPort = Global::props->getPropertyInt("Hypertable.RangeServer.port", DEFAULT_RANGESERVER_PORT);

    strcpy(base, rangeServers);

    ptr = strtok_r(base, " \t\r\n,", &last);
    while (ptr != 0) {
      rsInfoPtr.reset( new RangeServerInfoT );

      base = ptr;
      ptr = strchr(base, ':');
      if (ptr != 0) {
	rsInfoPtr->hostname = std::string(base, ptr-base);
	port = (uint16_t)atoi(ptr+1);
      }
      else {
	rsInfoPtr->hostname = ptr;
	port = (uint16_t)defaultPort;
      }

      memset(&addr, 0, sizeof(struct sockaddr_in));
      {
	struct hostent *he = gethostbyname(rsInfoPtr->hostname.c_str());
	if (he == 0) {
	  herror("gethostbyname()");
	  exit(1);
	}
	memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
      }
      addr.sin_family = AF_INET;
      addr.sin_port = htons(port);

      waitMessage = (std::string)"Waiting for RangeServer at " + rsInfoPtr->hostname + ":" + port;
      rsInfoPtr->connManager = new ConnectionManager(waitMessage);

      rsInfoPtr->connManager->Initiate(Global::comm, addr, 20);

      if (!rsInfoPtr->connManager->WaitForConnection(45))
	return false;

      Global::rangeServerInfo.push_back(rsInfoPtr);
      ptr = strtok_r(0, " \t\r\n,", &last);
    }

    return true;
  }

}
