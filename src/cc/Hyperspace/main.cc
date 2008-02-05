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

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionHandlerFactory.h"

#include "ServerConnectionHandler.h"
#include "ServerKeepaliveHandler.h"
#include "Master.h"

using namespace Hyperspace;
using namespace Hypertable;
using namespace std;


namespace {
  const char *usage[] = {
    "usage: Hyperspace.Master [OPTIONS]",
    "",
    "OPTIONS:",
    "  --config=<file>   Read configuration from <file>.  The default config file is",
    "                    \"conf/hypertable.cfg\" relative to the toplevel install directory",
    "  --install-dir=<dir> Set the installation directory to <dir>",
    "  --pidfile=<fname> Write the process ID to <fname> upon successful startup",
    "  --help            Display this help text and exit",
    "  --verbose,-v      Generate verbose output",
    ""
    "This program is the Hyperspace master server.",
    (const char *)0
  };
  const int DEFAULT_WORKERS           = 20;
}



/**
 *
 */
class HandlerFactory : public ConnectionHandlerFactory {

public:
  HandlerFactory(Comm *comm, ApplicationQueuePtr &appQueuePtr, MasterPtr &masterPtr) : m_comm(comm), m_app_queue_ptr(appQueuePtr), m_master_ptr(masterPtr) {
    return;
  }
  virtual void get_instance(DispatchHandlerPtr &dhp) {
    dhp = new ServerConnectionHandler(m_comm, m_app_queue_ptr, m_master_ptr);
  }

private:
  Comm                 *m_comm;
  ApplicationQueuePtr   m_app_queue_ptr;
  MasterPtr             m_master_ptr;
};



/**
 * 
 */
int main(int argc, char **argv) {
  string configFile = "";
  string pidFile = "";
  PropertiesPtr props_ptr;
  bool verbose = false;
  MasterPtr masterPtr;
  int port, reactorCount, workerCount;
  Comm *comm;
  ApplicationQueuePtr appQueuePtr;
  ServerKeepaliveHandlerPtr keepaliveHandlerPtr;
  ConnectionManagerPtr connManagerPtr;
  int error;
  struct sockaddr_in localAddr;

  System::initialize(argv[0]);
  
  if (argc > 1) {
    for (int i=1; i<argc; i++) {
      if (!strncmp(argv[i], "--config=", 9))
	configFile = &argv[i][9];
      else if (!strncmp(argv[i], "--install-dir=", 14)) {
	System::installDir = &argv[i][14];
	if (System::installDir.find('/',  System::installDir.length()-1) != string::npos)
	  System::installDir = System::installDir.substr(0, System::installDir.length()-1);
      }
      else if (!strncmp(argv[i], "--pidfile=", 10))
	pidFile = &argv[i][10];
      else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
	verbose = true;
      else if (!strcmp(argv[i], "-?") || !strcmp(argv[i], "--help")) {
	Usage::dump_and_exit(usage);
      }
      else {
	cerr << "Hyperspace.master: Unrecognized argument '" << argv[i] << "'" << endl;
	
	exit(1);
      }
    }
  }

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  props_ptr = new Properties(configFile);
  if (verbose)
    props_ptr->set("Hypertable.Verbose", "true");

  port         = props_ptr->get_int("Hyperspace.Master.Port", Master::DEFAULT_MASTER_PORT);
  reactorCount = props_ptr->get_int("Hyperspace.Master.Reactors", System::get_processor_count());
  workerCount  = props_ptr->get_int("Hyperspace.Master.Workers", DEFAULT_WORKERS);

  ReactorFactory::initialize(reactorCount);

  comm = new Comm();

  connManagerPtr = new ConnectionManager(comm);

  if (verbose) {
    cout << "CPU count = " << System::get_processor_count() << endl;
    cout << "Hyperspace.Master.Port=" << port << endl;
    cout << "Hyperspace.Master.Workers=" << workerCount << endl;
    cout << "Hyperspace.Master.Reactors=" << reactorCount << endl;
  }

  InetAddr::initialize(&localAddr, INADDR_ANY, port);

  masterPtr = new Master(connManagerPtr, props_ptr, keepaliveHandlerPtr);
  appQueuePtr = new ApplicationQueue(workerCount);

  ConnectionHandlerFactoryPtr chfPtr( new HandlerFactory(comm, appQueuePtr, masterPtr) );
  if ((error = comm->listen(localAddr, chfPtr)) != Error::OK) {
    std::string str;
    HT_ERRORF("Unable to listen for connections on %s - %s", 
		 InetAddr::string_format(str, localAddr), Error::get_text(error));
    exit(1);
  }

  DispatchHandlerPtr dhp(keepaliveHandlerPtr.get());

  if ((error = comm->create_datagram_receive_socket(&localAddr, dhp)) != Error::OK) {
    std::string str;
    HT_ERRORF("Unable to create datagram receive socket %s - %s", 
		 InetAddr::string_format(str, localAddr), Error::get_text(error));
    exit(1);
  }

  if (pidFile != "") {
    fstream filestr (pidFile.c_str(), fstream::out);
    filestr << getpid() << endl;
    filestr.close();
  }

  appQueuePtr->join();

  appQueuePtr->shutdown();

  delete comm;
  return 0;
}
