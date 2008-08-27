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

#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/CommandShell.h"
#include "Hypertable/Lib/HqlCommandInterpreter.h"
#include "Hypertable/Lib/RangeServerClient.h"

#include "RangeServerCommandInterpreter.h"

using namespace Hypertable;
using namespace std;

namespace {
  const char *usage_str =
  "\n" \
  "usage: rsclient [OPTIONS] <host>[:<port>]\n" \
  "\n" \
  "OPTIONS";

  /**
   *
   */
  void build_inet_address(struct sockaddr_in &addr, PropertiesPtr &props_ptr, std::string &location) {
    std::string host;
    uint16_t port;
    size_t colon_offset;

    if ((colon_offset = location.find(":")) == string::npos) {
      host = location;
      port = (uint16_t)props_ptr->get_int("Hypertable.RangeServer.Port", 38060);
    }
    else {
      host = location.substr(0, colon_offset);
      port = atoi(location.substr(colon_offset+1).c_str());
    }

    memset(&addr, 0, sizeof(struct sockaddr_in));
    {
      struct hostent *he = gethostbyname(host.c_str());
      if (he == 0) {
        herror(host.c_str());
        exit(1);
      }
      memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
  }

  class NullDispatchHandler : public DispatchHandler {
  public:
    virtual void handle(EventPtr &event_ptr) { return; }
  };

}

int main(int argc, char **argv) {
  CommandShellPtr shell;
  CommandInterpreterPtr interp;
  string cfgfile = "";
  Comm *comm = 0;
  int error = 1;
  Hyperspace::SessionPtr hyperspace;
  RangeServerClientPtr client;
  struct sockaddr_in addr;
  DispatchHandlerPtr null_handler_ptr = new NullDispatchHandler();
  PropertiesPtr props_ptr;
  std::string location_str;

  try {
    Config::Desc generic(usage_str);
    CommandShell::add_options(generic);

    // Hidden options: server location
    Config::Desc hidden("Hidden options");
    hidden.add_options()
      ("server-location", Config::value<string>(), "server location");

    Config::PositionalDesc p;
    p.add("server-location", -1);

    Config::init_with_comm(argc, argv, &generic, &hidden, &p);

    if (Config::varmap.count("server-location") == 0) {
      cout << Config::description() << "\n";
      return 1;
    }

    props_ptr = new Properties(Config::cfgfile);

    location_str = Config::varmap["server-location"].as<string>();

    build_inet_address(addr, props_ptr, location_str);

    comm = Comm::instance();

    // Create Range Server client object
    client = new RangeServerClient(comm, 30);

    // connect to RangeServer
    if ((error = comm->connect(addr, null_handler_ptr)) != Error::OK) {
      cerr << "error: unable to connect to RangeServer at " << endl;
      exit(1);
    }

    // Connect to Hyperspace
    hyperspace = new Hyperspace::Session(comm, props_ptr, 0);
    if (!hyperspace->wait_for_connection(30))
      exit(1);

    interp = new RangeServerCommandInterpreter(comm, hyperspace, addr, client);

    shell = new CommandShell("rsclient", interp, Config::varmap);

    error = shell->run();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
  catch(exception& e) {
    HT_ERROR_OUT << e.what() << HT_END;
  }

  return error;
}
