/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
#include <cstring>
#include <iostream>
#include <string>

#include "Common/Init.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorRunner.h"

#include "Config.h"
#include "ConnectionHandler.h"
#include "Global.h"
#include "HyperspaceSessionHandler.h"
#include "RangeServer.h"
#include "TimerHandler.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

typedef Meta::list<RangeServerPolicy, DfsClientPolicy, HyperspaceClientPolicy,
        MasterClientPolicy, DefaultServerPolicy> Policies;

int main(int argc, char **argv) {

  ReactorRunner::ms_record_arrival_clocks = true;

  try {
    init_with_policies<Policies>(argc, argv);

    Global::verbose = get_bool("verbose");

    if (has("induce-failure")) {
      if (Global::failure_inducer == 0)
        Global::failure_inducer = new FailureInducer();

      Global::failure_inducer->parse_option(get_str("induce-failure"));
    }

    Comm *comm = Comm::instance();
    ConnectionManagerPtr conn_manager= new ConnectionManager(comm);

    int worker_count = get_i32("Hypertable.RangeServer.Workers");
    ApplicationQueuePtr app_queue = new ApplicationQueue(worker_count);

    /**
     * Connect to Hyperspace
     */
    HyperspaceSessionHandler hs_handler;
    Global::hyperspace = new Hyperspace::Session(comm, properties);
    Global::hyperspace->add_callback(&hs_handler);
    int timeout = get_i32("Hyperspace.Timeout");

    if (!Global::hyperspace->wait_for_connection(timeout)) {
      HT_ERROR("Unable to connect to hyperspace, exiting...");
      exit(1);
    }

    RangeServerPtr range_server= new RangeServer(properties,
        conn_manager, app_queue, Global::hyperspace);

    // install maintenance timer
    TimerHandlerPtr timer_handler = new TimerHandler(comm, range_server.get());

    app_queue->join();

    HT_ERROR("Exiting RangeServer.");
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
