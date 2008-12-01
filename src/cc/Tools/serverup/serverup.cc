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

#include "Common/Config.h"
#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorFactory.h"

#include "DfsBroker/Lib/Client.h"

#include "Hyperspace/Session.h"
#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/RangeServerClient.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace {

  const char *usage =
    "usage: serverup [options] <server-name>\n\n"
    "Description:\n"
    "  This program checks to see if the server, specified by <server-name>\n"
    "  is up. return 0 if true, 1 otherwise. <server-name> may be one of the\n"
    "  following values: dfsbroker, hyperspace, master rangeserver\n\n"
    "Options";

  struct AppPolicy : Config::Policy {
    static void init_options() {
      cmdline_desc(usage).add_options()
          ("wait", i32()->default_value(2000), "Check wait time in seconds");
      cmdline_hidden_desc().add_options()("server-name", str(), "");
      cmdline_positional_desc().add("server-name", -1);
    }
    static void init() {
      // we want to override the default behavior that verbose
      // turns on debugging by clearing the defaulted flag
      if (defaulted("logging-level"))
        properties->set("logging-level", String("fatal"));
    }
  };

  typedef Meta::list<AppPolicy, DfsClientPolicy, HyperspaceClientPolicy,
          MasterClientPolicy, RangeServerClientPolicy, DefaultCommPolicy>
          Policies;

  void check_dfsbroker(ConnectionManagerPtr &conn_mgr, uint32_t wait_millis) {
    HT_DEBUG_OUT <<"Checking dfsbroker at "<< get_str("dfs-host")
                 <<':'<< get_i16("dfs-port") << HT_END;
    DfsBroker::Client *dfs = new DfsBroker::Client(conn_mgr, properties);

    if (!dfs->wait_for_connection(wait_millis))
      HT_THROW(Error::REQUEST_TIMEOUT, "connecting to dfsbroker");

    HT_TRY("getting dfsbroker status", dfs->status());
  }

  Hyperspace::SessionPtr hyperspace;

  void check_hyperspace(ConnectionManagerPtr &conn_mgr, uint32_t max_wait_millis) {
    HT_DEBUG_OUT <<"Checking hyperspace at "<< get_str("hs-host")
                 <<':'<< get_i16("rs-port") << HT_END;
    int error;
    hyperspace = new Hyperspace::Session(conn_mgr->get_comm(), properties, 0);

    if (!hyperspace->wait_for_connection(max_wait_millis))
      HT_THROW(Error::REQUEST_TIMEOUT, "connecting to hyperspace");

    if ((error = hyperspace->status()) != Error::OK)
      HT_THROW(error, "getting hyperspace status");
  }

  void check_master(ConnectionManagerPtr &conn_mgr, uint32_t wait_millis) {
    HT_DEBUG_OUT <<"Checking master via hyperspace"<< HT_END;

    if (!hyperspace) {
      hyperspace = new Hyperspace::Session(conn_mgr->get_comm(), properties, 0);

      if (!hyperspace->wait_for_connection(wait_millis))
        HT_THROW(Error::REQUEST_TIMEOUT, "connecting to hyperspace");
    }
    ApplicationQueuePtr app_queue = new ApplicationQueue(1);
    MasterClient *master = new MasterClient(conn_mgr, hyperspace,
        get_i32("master-timeout"), app_queue);
    master->set_verbose_flag(false);

    int error;

    if ((error = master->initiate_connection(0)) != Error::OK)
      HT_THROW(error, "initiating master connection");

    if (!master->wait_for_connection(wait_millis))
      HT_THROW(Error::REQUEST_TIMEOUT, "connecting to master");

    if ((error = master->status()) != Error::OK)
      HT_THROW(error, "getting master status");
  }

  void check_rangeserver(ConnectionManagerPtr &conn_mgr, uint32_t wait_millis) {
    int rs_timeout = get_i32("range-server-timeout");
    InetAddr addr(get_str("rs-host"), get_i16("rs-port"));
    HT_DEBUG_OUT <<"Checking rangeserver at "<< addr << HT_END;

    conn_mgr->add(addr, rs_timeout, "Range Server");

    if (!conn_mgr->wait_for_connection(addr, wait_millis))
      HT_THROW(Error::REQUEST_TIMEOUT, "connecting to range server");

    RangeServerClient *range_server =
        new RangeServerClient(conn_mgr->get_comm(), rs_timeout);
    range_server->status(addr);
  }

} // local namespace

#define HT_CHECK_SERVER(_server_) do { \
  try { check_##_server_(conn_mgr, wait_millis); } catch (Exception &e) { \
    if (verbose) { \
      HT_DEBUG_OUT << e << HT_END; \
      cout << #_server_ <<" - down" << endl; \
    } \
    ++down; \
    break; \
  } \
  if (verbose) cout << #_server_ <<" - up" << endl; \
} while (0)

/**
 *
 */
int main(int argc, char **argv) {
  int down = 0;

  try {
    init_with_policies<Policies>(argc, argv);

    bool silent = get_bool("silent");
    uint32_t wait_millis = get_i32("wait");
    String server_name = get("server-name", String());
    bool verbose = get_bool("verbose");

    ConnectionManagerPtr conn_mgr = new ConnectionManager();
    conn_mgr->set_quiet_mode(silent);

    if (server_name == "dfsbroker") {
      HT_CHECK_SERVER(dfsbroker);
    }
    else if (server_name == "hyperspace") {
      HT_CHECK_SERVER(hyperspace);
    }
    else if (server_name == "master") {
      HT_CHECK_SERVER(master);
    }
    else if (server_name == "rangeserver") {
      HT_CHECK_SERVER(rangeserver);
    }
    else {
      HT_CHECK_SERVER(dfsbroker);
      HT_CHECK_SERVER(hyperspace);
      HT_CHECK_SERVER(master);
      HT_CHECK_SERVER(rangeserver);
    }
    
    // Without these, I'm seeing SEGFAULTS on exit
    hyperspace = 0;
    properties = 0;

    if (!silent)
      cout << (down ? "false" : "true") << endl;
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return down;
}
