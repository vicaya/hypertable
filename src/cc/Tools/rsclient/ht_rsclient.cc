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
#include "Common/Init.h"
#include "Common/InetAddr.h"

#include <iostream>

#include "AsyncComm/DispatchHandler.h"

#include "Hyperspace/Session.h"

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

  const char *usage =
    "\n"
    "Usage: ht_rsclient [options] <host>[:<port>]\n\nOptions"
    ;

  struct AppPolicy : Policy {
    static void init_options() {
      cmdline_desc(usage).add_options()
        ("no-hyperspace", "Do not establish a connection to hyperspace")
        ;
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

  class RangeServerDispatchHandler : public DispatchHandler {
  public:
    RangeServerDispatchHandler() : m_connected(false) { return; }
    virtual void handle(EventPtr &event_ptr) {
      if (event_ptr->type == Event::DISCONNECT) {
        if (!m_connected) {
          cout << "Unable to establish connection to range server" << endl;
          _exit(0);
        }
      }
      else if (event_ptr->type == Event::CONNECTION_ESTABLISHED)
        m_connected = true;
    }
  private:
    bool m_connected;
  };

} // local namespace


int main(int argc, char **argv) {
  int error = 1;
  bool no_hyperspace = false;

  try {
    init_with_policies<Policies>(argc, argv);

    int timeout = get_i32("timeout");
    InetAddr addr(get_str("rs-host"), get_i16("rs-port"));

    if (has("no-hyperspace"))
      no_hyperspace = true;

    Comm *comm = Comm::instance();

    // Create Range Server client object
    RangeServerClientPtr client = new RangeServerClient(comm, timeout);

    DispatchHandlerPtr dispatch_handler_ptr = new RangeServerDispatchHandler();
    // connect to RangeServer
    if ((error = comm->connect(addr, dispatch_handler_ptr)) != Error::OK) {
      cerr << "error: unable to connect to RangeServer "<< addr << endl;
      _exit(1);
    }

    poll(0, 0, 100);

    // Maybe connect to Hyperspace
    Hyperspace::SessionPtr hyperspace;
    if (!no_hyperspace) {
      hyperspace = new Hyperspace::Session(comm, properties);
      if (!hyperspace->wait_for_connection(timeout))
        _exit(1);
    }

    CommandInterpreterPtr interp =
        new RangeServerCommandInterpreter(comm, hyperspace, addr, client);

    CommandShellPtr shell = new CommandShell("rsclient", interp, properties);

    error = shell->run();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit( e.code() );
  }
  _exit(error);
}
