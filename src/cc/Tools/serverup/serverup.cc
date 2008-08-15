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

extern "C" {
#include <netdb.h>
}

#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorFactory.h"

#include "DfsBroker/Lib/Client.h"

#include "Hyperspace/Session.h"
#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/RangeServerClient.h"

using namespace Hypertable;
using namespace std;

namespace {

  const char *usage[] = {
    "usage: serverup [options] <servername>",
    "",
    "  options:",
    "    --config=<file>  Read configuration from <file>.  The default file",
    "                     is \"conf/hypertable.cfg\" relative to the toplevel",
    "                     install directory",
    "    --wait=<sec>     wait seconds until give up",
    "    --host=<name>    Connect to the server running on host <name>",
    "    --port=<n>       Connect to the server running on port <n>",
    "    --help           Display this help text and exit",
    "    --verbose,-v     Display 'true' if up, 'false' otherwise",
    "",
    "  This program check checks to see if the server, named <servername>,",
    "  is up.  It first determines the host and port that the server is",
    "  listening on from the command line options, or if not supplied, the",
    "  the config file.  It then establishes a connection with the server and,",
    "  sends a STATUS request.  If the response from the STATUS request is OK,",
    "  then the program exits with status of 0.  If a failure occurs,",
    "  or a non-OK error code is returned from the STATUS request, the program",
    "  terminates with an exit status of 1.  <servername> may be one of the",
    "  following values:",
    "",
    "  dfsbroker",
    "  hyperspace",
    "  master",
    "  rangeserver",
    "",
    (const char *)0
  };

}


/**
 *
 */
int main(int argc, char **argv) {
  string cfgfile = "";
  string server_name = "";
  string host_name = "";
  int port = 0;
  PropertiesPtr props_ptr;
  struct sockaddr_in addr;
  const char *host_prop = 0;
  const char *port_prop = 0;
  const char *port_str = 0;
  Comm *comm;
  ConnectionManagerPtr conn_mgr;
  DfsBroker::Client *client;
  Hyperspace::SessionPtr hyperspace;
  RangeServerClient *range_server;
  ApplicationQueuePtr app_queue;
  int error;
  bool silent = false;
  int wait_secs = 2;

  try {

    System::initialize(System::locate_install_dir(argv[0]));
    ReactorFactory::initialize((uint16_t)System::get_processor_count());

    for (int i=1; i<argc; i++) {
      if (!strncmp(argv[i], "--config=", 9))
        cfgfile = &argv[i][9];
      else if (!strncmp(argv[i], "--wait=", 7))
        wait_secs = atoi(&argv[i][7]);
      else if (!strncmp(argv[i], "--host=", 7))
        host_name = &argv[i][7];
      else if (!strncmp(argv[i], "--port=", 7))
        port_str = &argv[i][7];
      else if (!strcmp(argv[i], "--silent"))
        silent = true;
      else if (argv[i][0] == '-' || server_name != "")
        Usage::dump_and_exit(usage);
      else
        server_name = argv[i];
    }

    if (cfgfile == "")
      cfgfile = System::install_dir + "/conf/hypertable.cfg";

    props_ptr = new Properties(cfgfile);

    if (server_name == "dfsbroker") {
      host_prop = "DfsBroker.Host";
      port_prop = "DfsBroker.Port";
    }
    else if (server_name == "hyperspace") {
      host_prop = "Hyperspace.Master.Host";
      port_prop = "Hyperspace.Master.Port";
    }
    else if (server_name == "master") {
      host_prop = "Hypertable.Master.Host";
      port_prop = "Hypertable.Master.Port";
    }
    else if (server_name == "rangeserver") {
      if (host_name == "")
        host_name = "localhost";
      port_prop = "Hypertable.RangeServer.Port";
    }
    else
      Usage::dump_and_exit(usage);

    {
      if (host_name == "")
        host_name = props_ptr->get(host_prop, "localhost");
      else if (server_name != "rangeserver")
        props_ptr->set(host_prop, host_name.c_str());

      if (port_str != 0)
        props_ptr->set(port_prop, port_str);

      port = props_ptr->get_int(port_prop, 0);
      if (port == 0 || port < 1024 || port >= 65536) {
        HT_ERRORF("%s not specified or out of range : %d", port_prop, port);
        return 1;
      }

      if (!InetAddr::initialize(&addr, host_name.c_str(), (uint16_t)port))
        HT_THROWF(Error::COMMAND_PARSE_ERROR, "Unable to construct address "
                  "from host=%s port=%u", host_name.c_str(), port);
    }

    props_ptr->set("silent", "true");

    comm = Comm::instance();
    conn_mgr = new ConnectionManager(comm);
    conn_mgr->set_quiet_mode(true);

    if (server_name == "dfsbroker") {
      client = new DfsBroker::Client(conn_mgr, addr, 30);
      if (!client->wait_for_connection(wait_secs))
        goto abort;
      client->status();
    }
    else if (server_name == "hyperspace") {
      hyperspace = new Hyperspace::Session(conn_mgr->get_comm(), props_ptr, 0);
      if (!hyperspace->wait_for_connection(wait_secs))
        goto abort;
      if ((error = hyperspace->status()) != Error::OK)
        goto abort;
    }
    else if (server_name == "master") {
      MasterClientPtr  master;

      hyperspace = new Hyperspace::Session(conn_mgr->get_comm(), props_ptr, 0);

      if (!hyperspace->wait_for_connection(wait_secs))
        goto abort;

      app_queue = new ApplicationQueue(1);
      master = new MasterClient(conn_mgr, hyperspace, 30, app_queue);
      master->set_verbose_flag(false);
      if (master->initiate_connection(0) != Error::OK)
        goto abort;

      if (!master->wait_for_connection(wait_secs))
        goto abort;

      if ((error = master->status()) != Error::OK)
        goto abort;
    }
    else if (server_name == "rangeserver") {
      conn_mgr->add(addr, 30, "Range Server");
      if (!conn_mgr->wait_for_connection(addr, wait_secs))
        goto abort;
      range_server = new RangeServerClient(comm, 30);
      range_server->status(addr);
    }

    if (!silent)
      cout << "true" << endl;

    return 0;

  abort:
    if (!silent)
      cout << "false" << endl;
    return 1;

  }
  catch (Exception &e) {
    HT_ERRORF("%s - %s", Error::get_text(e.code()), e.what());
    return 1;
  }

  return 0;
}
