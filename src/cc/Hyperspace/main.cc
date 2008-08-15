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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
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
    "  --config=<file>   Read configuration from <file>.  The default file is",
    "                    \"conf/hypertable.cfg\" ",
    "  --install-dir=<dir> Set the installation directory to <dir>",
    "  --pidfile=<fname> Write the process ID to <fname> upon startup",
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
  HandlerFactory(Comm *comm, ApplicationQueuePtr &app_queue, MasterPtr &master)
    : m_comm(comm), m_app_queue_ptr(app_queue), m_master_ptr(master) { }

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
  string cfg_file = "";
  string pidfile = "";
  PropertiesPtr props_ptr;
  bool verbose = false;
  MasterPtr master;
  int port, reactor_count, worker_count;
  Comm *comm;
  ApplicationQueuePtr app_queue;
  ServerKeepaliveHandlerPtr keepalive_handler;
  ConnectionManagerPtr conn_mgr;
  struct sockaddr_in local_addr;

  System::initialize(System::locate_install_dir(argv[0]));

  if (argc > 1) {
    for (int i=1; i<argc; i++) {
      if (!strncmp(argv[i], "--config=", 9))
        cfg_file = &argv[i][9];
      else if (!strncmp(argv[i], "--install-dir=", 14)) {
        System::install_dir = &argv[i][14];
        if (System::install_dir.find('/',  System::install_dir.length()-1)
            != string::npos)
          System::install_dir = System::install_dir.substr(0,
              System::install_dir.length()-1);
      }
      else if (!strncmp(argv[i], "--pidfile=", 10))
        pidfile = &argv[i][10];
      else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
        verbose = true;
      else if (!strcmp(argv[i], "-?") || !strcmp(argv[i], "--help")) {
        Usage::dump_and_exit(usage);
      }
      else {
        cerr << "Hyperspace.master: Unrecognized argument '" << argv[i] << "'"
             << endl;
        exit(1);
      }
    }
  }

  if (cfg_file == "")
    cfg_file = System::install_dir + "/conf/hypertable.cfg";

  props_ptr = new Properties(cfg_file);
  if (verbose)
    props_ptr->set("Hypertable.Verbose", "true");

  port = props_ptr->get_int("Hyperspace.Master.Port",
                            Master::DEFAULT_MASTER_PORT);
  reactor_count = props_ptr->get_int("Hyperspace.Master.Reactors",
                                     System::get_processor_count());
  worker_count = props_ptr->get_int("Hyperspace.Master.Workers",
                                    DEFAULT_WORKERS);

  ReactorFactory::initialize(reactor_count);

  comm = Comm::instance();

  conn_mgr = new ConnectionManager(comm);

  if (verbose) {
    cout << "CPU count = " << System::get_processor_count() << endl;
    cout << "Hyperspace.Master.Port=" << port << endl;
    cout << "Hyperspace.Master.Workers=" << worker_count << endl;
    cout << "Hyperspace.Master.Reactors=" << reactor_count << endl;
  }

  InetAddr::initialize(&local_addr, INADDR_ANY, port);

  master = new Master(conn_mgr, props_ptr, keepalive_handler);
  app_queue = new ApplicationQueue(worker_count);

  ConnectionHandlerFactoryPtr chfp(new HandlerFactory(comm, app_queue, master));
  comm->listen(local_addr, chfp);

  DispatchHandlerPtr dhp(keepalive_handler.get());

  comm->create_datagram_receive_socket(&local_addr, 0x10, dhp);

  if (pidfile != "") {
    fstream filestr (pidfile.c_str(), fstream::out);
    filestr << getpid() << endl;
    filestr.close();
  }

  app_queue->join();

  app_queue->shutdown();

  return 0;
}
