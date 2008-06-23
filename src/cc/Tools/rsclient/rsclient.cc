/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#include <boost/program_options.hpp>
namespace po = boost::program_options;

#include "AsyncComm/DispatchHandler.h"

#include "Common/InetAddr.h"
#include "Common/System.h"

#include "Hyperspace/Session.h"

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
  CommandShellPtr command_shell_ptr;
  CommandInterpreterPtr interp_ptr;
  string cfgfile = "";
  Comm *comm = 0;
  int error = 1;
  Hyperspace::SessionPtr hyperspace_ptr;
  RangeServerClientPtr range_server_ptr;
  struct sockaddr_in addr;
  DispatchHandlerPtr null_handler_ptr = new NullDispatchHandler();
  PropertiesPtr props_ptr;
  std::string location_str;

  System::initialize(argv[0]);
  ReactorFactory::initialize((uint16_t)System::get_processor_count());

  try {

    po::options_description generic(usage_str);
    generic.add_options()
      ("help", "Display this help message")
      ;

    CommandShell::add_options(generic);

    // Hidden options: server location
    po::options_description hidden("Hidden options");
    hidden.add_options()
      ("server-location", po::value<string>(), "server location")
      ;

    po::options_description cmdline_options;
    cmdline_options.add(generic).add(hidden);

    po::positional_options_description p;
    p.add("server-location", -1);

    po::variables_map vm;
    store(po::command_line_parser(argc, argv).
          options(cmdline_options).positional(p).run(), vm);
    po::notify(vm);

    if (vm.count("help") || vm.count("server-location") == 0) {
      cout << generic << "\n";
      return 1;
    }

    if (vm.count("config"))
      cfgfile = vm["config"].as<string>();
    else
      cfgfile = System::install_dir + "/conf/hypertable.cfg";

    props_ptr = new Properties(cfgfile);

    location_str = vm["server-location"].as<string>();

    build_inet_address(addr, props_ptr, location_str);

    comm = new Comm();

    // Create Range Server client object
    range_server_ptr = new RangeServerClient(comm, 30);

    // connect to RangeServer
    if ((error = comm->connect(addr, null_handler_ptr)) != Error::OK) {
      cerr << "error: unable to connect to RangeServer at " << endl;
      exit(1);
    }

    // Connect to Hyperspace
    hyperspace_ptr = new Hyperspace::Session(comm, props_ptr, 0);
    if (!hyperspace_ptr->wait_for_connection(30))
      exit(1);

    interp_ptr = new RangeServerCommandInterpreter(comm, hyperspace_ptr, addr, range_server_ptr);

    command_shell_ptr = new CommandShell("rsclient", interp_ptr, vm);

    error = command_shell_ptr->run();

  }
  catch(exception& e) {
    cerr << "error: " << e.what() << "\n";
  }
  catch(...) {
    cerr << "Exception of unknown type!\n";
  }

  return error;
}
