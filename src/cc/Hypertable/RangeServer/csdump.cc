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
#include <string>
#include <vector>
#include <iostream>

#include <boost/algorithm/string.hpp>

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorFactory.h"

#include "Common/ByteString.h"
#include "Common/InetAddr.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "DfsBroker/Lib/Client.h"

#include "Hypertable/Lib/Key.h"

#include "Config.h"
#include "CellStoreV0.h"
#include "CellStoreScannerV0.h"
#include "CellStoreTrailer.h"
#include "Global.h"

using namespace Hypertable;
using namespace Config;
using namespace std;

namespace {

  struct AppPolicy : Config::Policy {
    static void init_options() {
      cmdline_desc("Usage: %s [options] <filename>\n\n"
        "Dumps the contents of the CellStore contained in the DFS <filename>."
        "\n\nOptions").add_options()
        ("all,a", "Dump everything, including key/value pairs")
        ("count,c", "Count the number of key/value pairs")
        ("end-key", str(), "Ignore keys that are greater than <arg>")
        ("start-key", str(), "Ignore keys that are less than or equal to <arg>")
        ;
      cmdline_hidden_desc().add_options()("filename", str(), "");
      cmdline_positional_desc().add("filename", -1);
    }
    static void init() {
      if (!has("filename")) {
        HT_ERROR_OUT <<"filename required" << HT_END;
        cout << cmdline_desc() << endl;
        exit(1);
      }
    }
  };

  typedef Meta::list<AppPolicy, DfsClientPolicy, DefaultCommPolicy> Policies;

} // local namespace


int main(int argc, char **argv) {
  try {
    init_with_policies<Policies>(argc, argv);

    bool dump_all = has("all");
    bool count_keys = has("count");
    String start_key = get("start-key", String());
    String end_key = get("end-key", String());
    bool got_end_key = has("end-key");
    bool hit_start = start_key.empty();
    int timeout = get_i32("timeout");
    String fname = get_str("filename");

    ConnectionManagerPtr conn_mgr = new ConnectionManager();

    DfsBroker::Client *dfs = new DfsBroker::Client(conn_mgr, properties);

    if (!dfs->wait_for_connection(timeout, 0)) {
      cerr << "error: timed out waiting for DFS broker" << endl;
      exit(1);
    }

    Global::block_cache = new FileBlockCache(200000000LL);

    /**
     * Open cellStore
     */
    CellStoreV0Ptr cellstore = new CellStoreV0(dfs);
    CellListScanner *scanner = 0;

    if (cellstore->open(fname.c_str(), 0, 0) != 0)
      return 1;

    if (cellstore->load_index() != 0)
      return 1;


    /**
     * Dump keys
     */
    uint64_t key_count = 0;
    ByteString key, value;
    Key key_comps;

    /**
     * Dump keys
     */
    if (dump_all || count_keys) {
      ScanContextPtr scan_ctx(new ScanContext());

      scanner = cellstore->create_scanner(scan_ctx);
      while (scanner->get(key_comps, value)) {

        if (!hit_start) {
          if (strcmp(key_comps.row, start_key.c_str()) <= 0) {
            scanner->forward();
            continue;
          }
          hit_start = true;
        }
        if (got_end_key && strcmp(key_comps.row, end_key.c_str()) > 0)
          break;
        if (count_keys)
          key_count++;
        else
          cout << key_comps << endl;
        scanner->forward();
      }
      delete scanner;
    }

    if (count_keys) {
      cout << key_count << endl;
      return 0;
    }

    /**
     * Dump block index
     */
    cout << endl;
    cout << "BLOCK INDEX:" << endl;
    cellstore->display_block_info();


    /**
     * Dump trailer
     */
    CellStoreTrailer *trailer = cellstore->get_trailer();

    cout << endl;
    cout << "TRAILER:" << endl;
    cout << *trailer;

    cout << endl;
    cout << "OTHER:" << endl;
    cout << "split row '" << cellstore->get_split_row() << "'" << endl;
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }

  return 0;
}
