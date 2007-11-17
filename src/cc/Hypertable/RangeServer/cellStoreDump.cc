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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
#include <string>
#include <vector>
#include <iostream>

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
#include "Global.h"

using namespace Hypertable;
using namespace std;

namespace {
  const uint16_t DEFAULT_DFSBROKER_PORT = 38030;

  const char *usage[] = {
    "usage: cellStoreDump [OPTIONS] <fname>",
    "",
    "OPTIONS:",
    "  --trailer-only  - Dump out the trailer information only",
    "",
    "Dumps the contents of the CellStore contained in the DFS file <fname>.",
    0
  };
}




int main(int argc, char **argv) {
  Comm *comm;
  ConnectionManagerPtr connManagerPtr;
  DfsBroker::Client *client;
  std::string fname = "";
  struct sockaddr_in addr;
  ByteString32T *key;
  ByteString32T *value;
  bool trailer_only = false;
  CellStorePtr cellStorePtr;

  ReactorFactory::initialize(1);
  System::initialize(argv[0]);

  for (int i=1; i<argc; i++) {
    if (!strcmp(argv[i], "--trailer-only"))
      trailer_only = true;
    else if (!strcmp(argv[i], "--help"))
      Usage::dump_and_exit(usage);
    else if (fname == "")
      fname = argv[i];
    else
      Usage::dump_and_exit(usage);
  }

  if (fname == "")
      Usage::dump_and_exit(usage);

  InetAddr::initialize(&addr, "localhost", DEFAULT_DFSBROKER_PORT);
  
  comm = new Comm();
  connManagerPtr = new ConnectionManager(comm);

  client = new DfsBroker::Client(connManagerPtr, addr, 20);
  if (!client->wait_for_connection(15)) {
    cerr << "error: timed out waiting for DFS broker" << endl;
    exit(1);
  }

  Global::blockCache = new FileBlockCache(200000000LL);

  /**
   * Open cellStore
   */
  cellStorePtr = new CellStoreV0(client);
  CellListScanner *scanner = 0;

  if (cellStorePtr->open(fname.c_str(), 0, 0) != 0)
    return 1;

  if (cellStorePtr->load_index() != 0)
    return 1;

  /**
   * Dump keys
   */
  if (!trailer_only) {
    ScanContextPtr scanContextPtr( new ScanContext(ScanContext::END_OF_TIME, 0) );

    scanner = cellStorePtr->create_scanner(scanContextPtr);
    while (scanner->get(&key, &value)) {
      cout << Key(key) << endl;
      scanner->forward();
    }
    delete scanner;
  }

  /**
   * Dump trailer
   */
  cout << "timestamp " << cellStorePtr->get_log_cutoff_time() << endl;
  cout << "split row '" << cellStorePtr->get_split_row() << "'" << endl;

  return 0;
}
