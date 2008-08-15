/**
 * Copyright (C) 2008 Donald <donaldliew@gmail.com>
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

#include "Hypertable/Lib/Stat.h"

using namespace Hypertable;
using namespace std;

namespace {

  const char *usage[] = {
    "usage: rsstat [options]",
    "",
    "  options:",
    "    --config=<file>  Read configuration from <file>.  The default file",
    "                     is \"conf/hypertable.cfg\" relative to the toplevel",
    "                     install directory",
    "    --host=<name>    Connect to the server running on host <name>",
    "    --port=<n>       Connect to the server running on port <n>",
    "    --help           Display this help text and exit",
    "    --verbose,-v     Display 'true' if up, 'false' otherwise",
    "",
    "  This program displays statistics of the specified range server.",
    "",
    (const char *)0
  };

}


/**
 *
 */
int main(int argc, char **argv) {
  string cfgfile = "";
  string host_name = "";
  int port = 0;
  PropertiesPtr props_ptr;
  struct sockaddr_in addr;
  const char *host_prop = 0;
  const char *port_prop = 0;
  const char *port_str = 0;
  Comm *comm = 0;
  ConnectionManagerPtr conn_mgr;
  Hyperspace::SessionPtr hyperspace;
  RangeServerClient *range_server;
  ApplicationQueuePtr app_queue;
  bool verbose = false;
  RangeServerStat stat;

  System::initialize(System::locate_install_dir(argv[0]));
  ReactorFactory::initialize((uint16_t)System::get_processor_count());

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      cfgfile = &argv[i][9];
    else if (!strncmp(argv[i], "--host=", 7))
      host_name = &argv[i][7];
    else if (!strncmp(argv[i], "--port=", 7))
      port_str = &argv[i][7];
    else if (!strcmp(argv[i], "--verbose") || !strcmp(argv[i], "-v"))
      verbose = true;
    else
      Usage::dump_and_exit(usage);
  }

  if (cfgfile == "")
    cfgfile = System::install_dir + "/conf/hypertable.cfg";

  props_ptr = new Properties(cfgfile);

  if (host_name == "")
    host_name = "localhost";
  port_prop = "Hypertable.RangeServer.Port";

  {
    if (host_name == "")
      host_name = props_ptr->get(host_prop, "localhost");

    if (port_str != 0)
      props_ptr->set(port_prop, port_str);

    port = props_ptr->get_int(port_prop, 0);
    if (port == 0 || port < 1024 || port >= 65536) {
      HT_ERRORF("%s not specified or out of range : %d", port_prop, port);
      return 1;
    }

    if (!InetAddr::initialize(&addr, host_name.c_str(), (uint16_t)port))
      goto abort;
  }

  props_ptr->set("silent", "true");

  comm = Comm::instance();
  conn_mgr = new ConnectionManager(comm);

  conn_mgr->add(addr, 30, "Range Server");
  if (!conn_mgr->wait_for_connection(addr, 2))
    goto abort;
  range_server = new RangeServerClient(comm, 30);

  range_server->get_statistics(addr, stat);

  cout << stat << endl;

  return 0;

 abort:
  return 1;
}
