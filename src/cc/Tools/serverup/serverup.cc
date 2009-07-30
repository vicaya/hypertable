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
#include "Common/Init.h"
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

#ifdef HT_WITH_THRIFT
# include "ThriftBroker/Config.h"
# include "ThriftBroker/Client.h"
#endif

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace {

  const char *usage =
    "Usage: serverup [options] <server-name>\n\n"
    "Description:\n"
    "  This program checks to see if the server, specified by <server-name>\n"
    "  is up. return 0 if true, 1 otherwise. <server-name> may be one of the\n"
    "  following values: dfsbroker, hyperspace, master rangeserver\n\n"
    "Options";

  struct AppPolicy : Config::Policy {
    static void init_options() {
      cmdline_desc(usage).add_options()
          ("wait", i32()->default_value(2000), "Check wait time in ms");
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

#ifdef HT_WITH_THRIFT
  typedef Meta::list<AppPolicy, DfsClientPolicy, HyperspaceClientPolicy,
          MasterClientPolicy, RangeServerClientPolicy, ThriftClientPolicy,
          DefaultCommPolicy> Policies;
#else
  typedef Meta::list<AppPolicy, DfsClientPolicy, HyperspaceClientPolicy,
          MasterClientPolicy, RangeServerClientPolicy, DefaultCommPolicy>
          Policies;
#endif

  void
  wait_for_connection(const char *server, ConnectionManagerPtr &conn_mgr,
                      InetAddr &addr, int timeout_ms, int wait_ms) {
    HT_DEBUG_OUT <<"Checking "<< server <<" at "<< addr << HT_END;

    conn_mgr->add(addr, timeout_ms, server);

    if (!conn_mgr->wait_for_connection(addr, wait_ms))
      HT_THROWF(Error::REQUEST_TIMEOUT, "connecting to %s", server);
  }

  void check_dfsbroker(ConnectionManagerPtr &conn_mgr, uint32_t wait_ms) {
    HT_DEBUG_OUT <<"Checking dfsbroker at "<< get_str("dfs-host")
                 <<':'<< get_i16("dfs-port") << HT_END;
    DfsBroker::Client *dfs = new DfsBroker::Client(conn_mgr, properties);

    if (!dfs->wait_for_connection(wait_ms))
      HT_THROW(Error::REQUEST_TIMEOUT, "connecting to dfsbroker");

    HT_TRY("getting dfsbroker status", dfs->status());
  }

  Hyperspace::Session *hyperspace;

  void check_hyperspace(ConnectionManagerPtr &conn_mgr, uint32_t max_wait_ms) {
    HT_DEBUG_OUT <<"Checking hyperspace at "<< get_str("hs-host")
                 <<':'<< get_i16("hs-port") << HT_END;
    Timer timer(max_wait_ms, true);
    int error;
    hyperspace = new Hyperspace::Session(conn_mgr->get_comm(), properties);

    if (!hyperspace->wait_for_connection(max_wait_ms))
      HT_THROW(Error::REQUEST_TIMEOUT, "connecting to hyperspace");

    if ((error = hyperspace->status(&timer)) != Error::OK)
      HT_THROW(error, "getting hyperspace status");
  }

  void check_master(ConnectionManagerPtr &conn_mgr, uint32_t wait_ms) {
    HT_DEBUG_OUT <<"Checking master via hyperspace"<< HT_END;
    Timer timer(wait_ms, true);

    if (!hyperspace) {
      hyperspace = new Hyperspace::Session(conn_mgr->get_comm(), properties);

      if (!hyperspace->wait_for_connection(wait_ms))
        HT_THROW(Error::REQUEST_TIMEOUT, "connecting to hyperspace");
    }
    ApplicationQueuePtr app_queue = new ApplicationQueue(1);
    Hyperspace::SessionPtr hyperspace_ptr = hyperspace;
    MasterClient *master = new MasterClient(conn_mgr, hyperspace_ptr,
                                            wait_ms, app_queue);
    master->set_verbose_flag(get_bool("verbose"));

    master->initiate_connection(0);

    if (!master->wait_for_connection(wait_ms))
      HT_THROW(Error::REQUEST_TIMEOUT, "connecting to master");

    master->status(&timer);
  }

  void check_rangeserver(ConnectionManagerPtr &conn_mgr, uint32_t wait_ms) {
    InetAddr addr(get_str("rs-host"), get_i16("rs-port"));

    wait_for_connection("range server", conn_mgr, addr, wait_ms, wait_ms);

    RangeServerClient *range_server =
      new RangeServerClient(conn_mgr->get_comm(), wait_ms);
    range_server->set_timeout(wait_ms);
    range_server->status(addr);
  }

  void check_thriftbroker(ConnectionManagerPtr &conn_mgr, int wait_ms) {
#ifdef HT_WITH_THRIFT
    int32_t id = -1;
    int timeout = get_i32("thrift-timeout");
    InetAddr addr(get_str("thrift-host"), get_i16("thrift-port"));

    wait_for_connection("thrift broker", conn_mgr, addr, timeout, wait_ms);

    try {
      Thrift::Client client(get_str("thrift-host"), get_i16("thrift-port"));
      id = client.get_table_id("METADATA");
    }
    catch (ThriftGen::ClientException &e) {
      HT_THROW(e.code, e.message);
    }
    catch (std::exception &e) {
      HT_THROW(Error::EXTERNAL, e.what());
    }
    HT_EXPECT(id == 0, Error::INVALID_METADATA);
#else
    HT_THROW(Error::FAILED_EXPECTATION, "Thrift support not installed");
#endif
  }

} // local namespace

#define CHECK_SERVER(_server_) do { \
  try { check_##_server_(conn_mgr, wait_ms); } catch (Exception &e) { \
    if (verbose) { \
      HT_DEBUG_OUT << e << HT_END; \
      cout << #_server_ <<" - down" << endl; \
    } \
    ++down; \
    break; \
  } \
  if (verbose) cout << #_server_ <<" - up" << endl; \
} while (0)


int main(int argc, char **argv) {
  int down = 0;

  try {
    init_with_policies<Policies>(argc, argv);

    bool silent = get_bool("silent");
    uint32_t wait_ms = get_i32("wait");
    String server_name = get("server-name", String());
    bool verbose = get_bool("verbose");

    ConnectionManagerPtr conn_mgr = new ConnectionManager();
    conn_mgr->set_quiet_mode(silent);

    if (server_name == "dfsbroker") {
      CHECK_SERVER(dfsbroker);
    }
    else if (server_name == "hyperspace") {
      CHECK_SERVER(hyperspace);
    }
    else if (server_name == "master") {
      CHECK_SERVER(master);
    }
    else if (server_name == "rangeserver") {
      CHECK_SERVER(rangeserver);
    }
    else if (server_name == "thriftbroker") {
      CHECK_SERVER(thriftbroker);
    }
    else {
      CHECK_SERVER(dfsbroker);
      CHECK_SERVER(hyperspace);
      CHECK_SERVER(master);
      CHECK_SERVER(rangeserver);
#ifdef HT_WITH_THRIFT
      CHECK_SERVER(thriftbroker);
#endif
    }

    if (!silent)
      cout << (down ? "false" : "true") << endl;
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);    // don't bother with global/static objects
  }
  _exit(down);   // ditto
}
