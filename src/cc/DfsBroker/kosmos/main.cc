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

#include "Common/Init.h"
#include "Common/FileUtils.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"

#include "DfsBroker/Lib/Config.h"
#include "DfsBroker/Lib/ConnectionHandlerFactory.h"

#include "KosmosBroker.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;

namespace KFS {
  extern std::string KFS_BUILD_VERSION_STRING;
}

struct AppPolicy : Config::Policy {
  static void init_options() {
    cmdline_desc().add_options()
      ("kfs-version", "Show KFS version and exit")
      ;
  }

  static void init() {
    alias("reactors", "Kfs.Broker.Reactors");
    alias("workers",  "Kfs.Broker.Workers");

    if (has("kfs-version")) {
      cout <<"  KFS: "<< KFS::KFS_BUILD_VERSION_STRING << endl;
      _exit(0);
    }
  }
};

typedef Meta::list<AppPolicy, DfsBrokerPolicy, DefaultCommPolicy> AppPolicies;

int main(int argc, char **argv) {
  try {
    init_with_policies<AppPolicies>(argc, argv);

    int port = get_i16("DfsBroker.Port");
    int worker_count  = get_i32("Kfs.Broker.Workers");
    ApplicationQueuePtr app_queue = new ApplicationQueue(worker_count);
    DfsBroker::BrokerPtr broker = new KosmosBroker(properties);
    ConnectionHandlerFactoryPtr chf(new DfsBroker::ConnectionHandlerFactory(
        Comm::instance(), app_queue, broker));
    InetAddr listen_addr(INADDR_ANY, port);

    Comm::instance()->listen(listen_addr, chf);
    app_queue->join();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
