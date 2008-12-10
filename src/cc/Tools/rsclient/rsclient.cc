/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include <iostream>

#include "Common/InetAddr.h"

#include "AsyncComm/DispatchHandler.h"

#include "Hyperspace/Session.h"

#include "Common/InetAddr.h"
#include "Tools/Lib/CommandShell.h"

#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/HqlCommandInterpreter.h"
#include "Hypertable/Lib/RangeServerClient.h"

#include "RangeServerCommandInterpreter.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace {

  struct AppPolicy : Policy {
    static void init_options() {
      cmdline_desc("Usage: rsclient [options] <host>[:<port>]\n\nOptions");
      cmdline_hidden_desc().add_options()("rs-location", str(), "");
      cmdline_positional_desc().add("rs-location", -1);
    }
    static void init() {
      if (has("rs-location")) {
        Endpoint e = InetAddr::parse_endpoint(get_str("rs-location"));
        properties->set("rs-host", e.host);
        if (e.port) properties->set("rs-port", e.port);
      }
    }
  };

  typedef Meta::list<AppPolicy, CommandShellPolicy, HyperspaceClientPolicy,
          RangeServerClientPolicy, DefaultCommPolicy> Policies;

  class NullDispatchHandler : public DispatchHandler {
  public:
    virtual void handle(EventPtr &event_ptr) { return; }
  };

} // local namespace


int main(int argc, char **argv) {
  int error = 1;

  try {
    init_with_policies<Policies>(argc, argv);

    int timeout = get_i32("timeout");
    InetAddr addr(get_str("rs-host"), get_i16("rs-port"));

    Comm *comm = Comm::instance();

    // Create Range Server client object
    RangeServerClientPtr client = new RangeServerClient(comm, timeout);

    DispatchHandlerPtr null_handler_ptr = new NullDispatchHandler();
    // connect to RangeServer
    if ((error = comm->connect(addr, null_handler_ptr)) != Error::OK) {
      cerr << "error: unable to connect to RangeServer "<< addr << endl;
      exit(1);
    }

    // Connect to Hyperspace
    Hyperspace::SessionPtr hyperspace =
        new Hyperspace::Session(comm, properties, 0);

    if (!hyperspace->wait_for_connection(timeout))
      exit(1);

    CommandInterpreterPtr interp =
        new RangeServerCommandInterpreter(comm, hyperspace, addr, client);

    CommandShellPtr shell = new CommandShell("rsclient", interp, properties);

    error = shell->run();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return e.code();
  }
  return error;
}
