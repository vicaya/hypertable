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

#include "CellStoreV0.h"
#include "CellStoreScannerV0.h"
#include "CellStoreTrailer.h"
#include "Global.h"

using namespace Hypertable;
using namespace std;

namespace {
  const uint16_t DEFAULT_DFSBROKER_PORT = 38030;

  const char *usage[] = {
    "usage: cellStoreDump [OPTIONS] <fname>",
    "",
    "OPTIONS:",
    "  -a                Dump everything, including key/value pairs",
    "  -c,--count        Count the number of key/value pairs",
    "  --end-key=<key>   Ignore keys that are greater than <key>",
    "  --start-key=<key> Ignore keys that are less than or equal to <key>",
    "",
    "Dumps the contents of the CellStore contained in the DFS file <fname>.",
    0
  };
}




int main(int argc, char **argv) {
  Comm *comm;
  ConnectionManagerPtr conn_mgr;
  DfsBroker::Client *client;
  std::string fname = "";
  ByteString key, value;
  bool dump_all = false;
  CellStoreV0Ptr cellstore;
  bool count_keys = false;
  uint64_t key_count = 0;
  std::string start_key, end_key;
  bool got_end_key = false;
  bool hit_start = false;
  PropertiesPtr props_ptr;
  string cfgfile = "";
  Key key_comps;

  ReactorFactory::initialize(1);
  System::initialize(argv[0]);

  for (int i=1; i<argc; i++) {
    if (!strcmp(argv[i], "-a"))
      dump_all = true;
    else if (!strcmp(argv[i], "-c") || !strcmp(argv[i], "--count"))
      count_keys = true;
    else if (!strncmp(argv[i], "--start-key=", 12)) {
      start_key = &argv[i][12];
      boost::trim_if(start_key, boost::is_any_of("'\""));
    }
    else if (!strncmp(argv[i], "--end-key=", 10)) {
      end_key = &argv[i][10];
      boost::trim_if(end_key, boost::is_any_of("'\""));
      got_end_key = true;
    }
    else if (!strcmp(argv[i], "--help"))
      Usage::dump_and_exit(usage);
    else if (fname == "")
      fname = argv[i];
    else
      Usage::dump_and_exit(usage);
  }

  if (fname == "")
      Usage::dump_and_exit(usage);

  if (cfgfile == "")
    cfgfile = System::install_dir + "/conf/hypertable.cfg";

  props_ptr = new Properties(cfgfile);

  if (start_key == "")
    hit_start = true;

  comm = new Comm();
  conn_mgr = new ConnectionManager(comm);

  client = new DfsBroker::Client(conn_mgr, props_ptr);
  if (!client->wait_for_connection(15)) {
    cerr << "error: timed out waiting for DFS broker" << endl;
    exit(1);
  }

  Global::block_cache = new FileBlockCache(200000000LL);

  /**
   * Open cellStore
   */
  cellstore = new CellStoreV0(client);
  CellListScanner *scanner = 0;

  if (cellstore->open(fname.c_str(), 0, 0) != 0)
    return 1;

  if (cellstore->load_index() != 0)
    return 1;


  /**
   * Dump keys
   */
  if (dump_all || count_keys) {
    ScanContextPtr scan_ctx(new ScanContext(END_OF_TIME));

    scanner = cellstore->create_scanner(scan_ctx);
    while (scanner->get(key, value)) {
      key_comps.load(key);

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

  return 0;
}
