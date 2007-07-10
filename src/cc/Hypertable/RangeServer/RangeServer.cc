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
#include <cassert>
#include <string>

#include <boost/shared_array.hpp>

extern "C" {
#include <arpa/inet.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <sys/types.h>
}

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/WorkQueue.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionHandlerFactory.h"
#include "AsyncComm/ReactorFactory.h"

#include "CommitLogLocal.h"
#include "ConnectionHandler.h"
#include "Global.h"
#include "MaintenanceThread.h"
#include "RangeServerProtocol.h"

using namespace hypertable;
using namespace std;


namespace {
  const char *usage[] = {
    "usage: Hypertable.RangeServer [OPTIONS]",
    "",
    "OPTIONS:",
    "  --config=<file>  Read configuration from <file>.  The default config file is",
    "                   \"conf/hypertable.cfg\" relative to the toplevel install directory",
    "  --help             Display this help text and exit",
    "  --metadata=<file>  Read METADATA simulation data from <file",
    "  --verbose,-v       Generate verbose output",
    ""
    "This program is the Hypertable range server.",
    (const char *)0
  };
  const int DEFAULT_PORT    = 38549;
  const int DEFAULT_WORKERS = 20;
  int Initialize();
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
  string metadataFile = "";
  int port, reactorCount, workerCount;

  System::Initialize(argv[0]);
  
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

  Global::props = new Properties(configFile);

  port = Global::props->getPropertyInt("Hypertable.RangeServer.port", DEFAULT_PORT);
  workerCount = Global::props->getPropertyInt("Hypertable.RangeServer.workers", DEFAULT_WORKERS);
  reactorCount = Global::props->getPropertyInt("Hypertable.RangeServer.reactors", System::GetProcessorCount());
  Global::rangeMaxBytes         = Global::props->getPropertyInt64("Hypertable.RangeServer.Range.MaxBytes", 200000000LL);
  Global::localityGroupMaxFiles  = Global::props->getPropertyInt("Hypertable.RangeServer.AccessGroup.MaxFiles", 10);
  Global::localityGroupMergeFiles  = Global::props->getPropertyInt("Hypertable.RangeServer.AccessGroup.MergeFiles", 4);
  Global::localityGroupMaxMemory = Global::props->getPropertyInt("Hypertable.RangeServer.AccessGroup.MaxMemory", 4000000);

  uint64_t blockCacheMemory = Global::props->getPropertyInt64("Hypertable.RangeServer.BlockCache.MaxMemory", 200000000LL);
  Global::blockCache = new FileBlockCache(blockCacheMemory);


  assert(Global::localityGroupMergeFiles <= Global::localityGroupMaxFiles);

  const char *dir = Global::props->getProperty("Hypertable.RangeServer.logDirRoot", 0);
  if (dir == 0) {
    cerr << "Hypertable.RangeServer.logDirRoot property not specified." << endl;
    exit(1);
  }
  if (dir[strlen(dir)-1] == '/')
    Global::logDirRoot = string(dir, strlen(dir)-1);
  else
    Global::logDirRoot = dir;

  if (Global::verbose) {
    cout << "CPU count = " << System::GetProcessorCount() << endl;
    cout << "Hypertable.RangeServer.port=" << port << endl;
    cout << "Hypertable.RangeServer.workers=" << workerCount << endl;
    cout << "Hypertable.RangeServer.reactors=" << reactorCount << endl;
    cout << "Hypertable.RangeServer.logDirRoot=" << Global::logDirRoot << endl;
    cout << "Hypertable.RangeServer.Range.MaxBytes=" << Global::rangeMaxBytes << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MaxFiles=" << Global::localityGroupMaxFiles << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MergeFiles=" << Global::localityGroupMergeFiles << endl;
    cout << "Hypertable.RangeServer.AccessGroup.MaxMemory=" << Global::localityGroupMaxMemory << endl;
    cout << "Hypertable.RangeServer.BlockCache.MaxMemory=" << blockCacheMemory << endl;
    cout << "METADATA file = '" << metadataFile << "'" << endl;
  }

  /**
   * Load METADATA simulation data
   */
  if (metadataFile != "")
    Global::metadata = Metadata::NewInstance(metadataFile.c_str());

  ReactorFactory::Initialize(reactorCount);

  Global::comm = new Comm(0);
  Global::protocol = new hypertable::RangeServerProtocol();

  /**
   * Create HYPERSPACE Client connection
   */
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

  if (Initialize() != Error::OK)
    goto done;

  // start commpaction thread
  {
    MaintenanceThread maintenanceThread;
    Global::maintenanceThreadPtr = new boost::thread(maintenanceThread);
  }

  Global::workQueue = new WorkQueue(workerCount);
  Global::comm->Listen(port, new HandlerFactory());
  Global::workQueue->Join();

 done:
  delete Global::props;
  delete Global::protocol;
  delete Global::workQueue;
  delete Global::hyperspace;
  delete Global::comm;
}


namespace {
  /**
   * - Figure out and create range server directory
   * - Clear any Range server state (including any partially created commit logs)
   * - Open the commit log
   */
  int Initialize() {
    char hostname[256];
    char ipStr[32];

    cerr << "RangeServer::Initialize enter" << endl;

    if (gethostname(hostname, 256) != 0) {
      LOG_VA_ERROR("gethostname() failed - %s", strerror(errno));
      exit(1);
    }

    struct hostent *he = gethostbyname(hostname);
    if (he == 0) {
      herror("gethostbyname()");
      exit(1);
    }

    strcpy(ipStr, inet_ntoa(*(struct in_addr *)he->h_addr_list[0]));

    int error;

    /**
     * Create /hypertable/servers directory
     */
    error = Global::hyperspace->Exists("/hypertable/servers");
    if (error == Error::HYPERTABLEFS_FILE_NOT_FOUND) {
      LOG_ERROR("Hypertablefs directory '/hypertable/servers' does not exist, try running 'Master --initialize' first");
      return -1;
    }
    else if (error != Error::OK) {
      LOG_VA_ERROR("Problem checking existence of '/hypertable/servers' hypertablefs directory - %s", Error::GetText(error));
      return -1;
    }

    cerr << "RangeServer found /hypertable/servers directory" << endl;

    struct timeval tval;
    if (gettimeofday(&tval, 0) != 0) {
      LOG_VA_ERROR("gettimeofday() failed - %s", strerror(errno));
      exit(1);
    }

    boost::shared_array<char> topDirPtr( new char [ 64 + strlen(ipStr) ] );
    char *topDir = topDirPtr.get();
    sprintf(topDir, "/hypertable/servers/%s_%ld", ipStr, tval.tv_sec);

    /**
     * Create /hypertable/servers/X.X.X.X_nnnnn directory
     */
    error = Global::hyperspace->Exists(topDir);
    if (error == Error::HYPERTABLEFS_FILE_NOT_FOUND) {
      error = Global::hyperspace->Mkdirs(topDir);
      if (error != Error::OK) {
	LOG_VA_ERROR("Problem creating hypertablefs directory '%s' - %s", topDir, Error::GetText(error));
	return -1;
      }
    }
    else if (error != Error::OK) {
      LOG_VA_ERROR("Problem checking existence of '%s' hypertablefs directory - %s", topDir, Error::GetText(error));
      return -1;
    }
    else {
      LOG_VA_ERROR("Hypertablefs directory '%s' already exists.", topDir);
      return -1;
    }

    Global::logDir = (string)topDir + "/logs/primary";

    /**
     * Create /hypertable/servers/X.X.X.X/logs/primary directory
     */
    string logDir = Global::logDirRoot + Global::logDir;

    if (!FileUtils::Mkdirs(logDir.c_str())) {
      LOG_VA_ERROR("Problem creating local log directory '%s'", logDir.c_str());
      return -1;
    }

    int64_t logFileSize = Global::props->getPropertyInt64("Hypertable.RangeServer.logFileSize", 0x100000000LL);
    if (Global::verbose) {
      cout << "Hypertable.RangeServer.logFileSize=" << logFileSize << endl;
      cout << "logDir=" << Global::logDir << endl;
    }

    Global::log = new CommitLogLocal(Global::logDirRoot, Global::logDir, logFileSize);    

    /**
     *  Register with Master
     */

    cerr << "RangeServer::Initialize exit" << endl;

    return 0;
  }

}
