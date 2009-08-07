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
#include <cstdlib>
#include <iostream>
#include <fstream>
#include <string>

extern "C" {
#include <poll.h>
#include <sys/types.h>
#include <unistd.h>
}

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/InetAddr.h"
#include "Common/Init.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"

#include "DfsBroker/Lib/Config.h"
#include "DfsBroker/Lib/ConnectionHandlerFactory.h"

#include "LocalBroker.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace {

struct AppPolicy : Policy {
  static void init_options() {
    cmdline_desc().add_options()
      ("root", str()->default_value("fs/local"), "root directory for local "
          "broker (if relative, it's relative to the installation directory")
      ;
    alias("port", "DfsBroker.Local.Port");
    alias("root", "DfsBroker.Local.Root");
    alias("workers", "DfsBroker.Local.Workers");
    alias("reactors", "DfsBroker.Local.Reactors");
  }
};

typedef Meta::list<AppPolicy, DfsBrokerPolicy, DefaultCommPolicy> Policies;

} // local namespace


int main(int argc, char **argv) {
  try {
    init_with_policies<Policies>(argc, argv);
    int port = get_i16("port");
    int worker_count = get_i32("workers");

    Comm *comm = Comm::instance();
    ApplicationQueuePtr app_queue = new ApplicationQueue(worker_count);
    BrokerPtr broker = new LocalBroker(properties);
    ConnectionHandlerFactoryPtr chfp =
        new DfsBroker::ConnectionHandlerFactory(comm, app_queue, broker);
    InetAddr listen_addr(INADDR_ANY, port);

    comm->listen(listen_addr, chfp);
    app_queue->join();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
