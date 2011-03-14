/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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

#include "Tools/Lib/CommandShell.h"

#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/HqlCommandInterpreter.h"
#include "Hypertable/Lib/MasterClient.h"

#include "MasterCommandInterpreter.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace {

  const char *usage =
    "\n"
    "Usage: ht_master_client [options] <host>[:<port>]\n\nOptions"
    ;

  struct AppPolicy : Policy {
    static void init_options() {
      cmdline_desc(usage);
      cmdline_hidden_desc().add_options()("master-location", str(), "");
      cmdline_positional_desc().add("master-location", -1);
    }
    static void init() {
      if (has("master-location")) {
        Endpoint e = InetAddr::parse_endpoint(get_str("master-location"));
        properties->set("master-host", e.host);
        if (e.port) properties->set("master-port", e.port);
      }
    }
  };

  typedef Meta::list<CommandShellPolicy, MasterClientPolicy,
                     DefaultCommPolicy, AppPolicy> Policies;

  class MasterDispatchHandler : public DispatchHandler {
  public:
    MasterDispatchHandler() : m_connected(false) { return; }
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

  try {
    init_with_policies<Policies>(argc, argv);

    int timeout = get_i32("timeout");
    InetAddr addr(get_str("master-host"), get_i16("master-port"));

    Comm *comm = Comm::instance();

    MasterClientPtr client = new MasterClient(comm, addr, timeout);

    DispatchHandlerPtr dispatch_handler_ptr = new MasterDispatchHandler();
    // connect to Master
    if ((error = comm->connect(addr, dispatch_handler_ptr)) != Error::OK) {
      cerr << "ERROR: unable to connect to Master "<< addr << endl;
      _exit(1);
    }

    poll(0, 0, 100);

    CommandInterpreterPtr interp =
        new MasterCommandInterpreter(comm, addr, client);

    CommandShellPtr shell = new CommandShell("master", interp, properties);

    error = shell->run();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit( e.code() );
  }
  _exit(error);
}
