/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
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
#include "AsyncComm/ReactorFactory.h"

#include "Common/ByteString.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "HdfsClient/HdfsClient.h"
#include "Hypertable/RangeServer/CellStoreV0.h"
#include "Hypertable/RangeServer/Global.h"
#include "Hypertable/RangeServer/Key.h"

using namespace hypertable;
using namespace std;

namespace {
  const uint16_t DEFAULT_HDFSBROKER_PORT = 38546;

  const char *usage[] = {
    "usage: cellStoreDump [OPTIONS] <fname>",
    "",
    "OPTIONS:",
    "  --trailer-only  - Dump out the trailer information only",
    "",
    "Dumps the contents of the CellStore contained in the HDFS file <fname>.",
    0
  };
}




int main(int argc, char **argv) {
  Comm *comm;
  HdfsClient *client;
  std::string fname = "";
  struct sockaddr_in addr;
  KeyT *key;
  ByteString32T *value;
  bool trailer_only = false;

  ReactorFactory::Initialize(1);
  System::Initialize(argv[0]);

  for (int i=1; i<argc; i++) {
    if (!strcmp(argv[i], "--trailer-only"))
      trailer_only = true;
    else if (!strcmp(argv[i], "--help"))
      Usage::DumpAndExit(usage);
    else if (fname == "")
      fname = argv[i];
    else
      Usage::DumpAndExit(usage);
  }

  if (fname == "")
      Usage::DumpAndExit(usage);

  /**
   *  Setup
   */
  memset(&addr, 0, sizeof(struct sockaddr_in));
  {
    struct hostent *he = gethostbyname("localhost");
    if (he == 0) {
      herror("gethostbyname()");
      exit(1);
    }
    memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
  }
  addr.sin_family = AF_INET;
  addr.sin_port = htons(DEFAULT_HDFSBROKER_PORT);

  comm = new Comm(0);

  client = new HdfsClient(comm, addr, 20);
  if (!client->WaitForConnection(10)) {
    cerr << "error: timed out waiting for HDFS broker" << endl;
    exit(1);
  }

  Global::blockCache = new FileBlockCache(200000000LL);

  /**
   * Open cellStore
   */
  CellStoreV0 *cellStore = new CellStoreV0(client);
  CellListScanner *scanner = 0;

  if (cellStore->Open(fname.c_str(), 0, 0) != 0)
    return 1;

  if (cellStore->LoadIndex() != 0)
    return 1;

  /**
   * Dump keys
   */
  if (!trailer_only) {
    cellStore->LockShareable();
    scanner = cellStore->CreateScanner();
    scanner->Reset();
    while (scanner->Get(&key, &value)) {
      cout << *key << endl;
      scanner->Forward();
    }
    delete scanner;
    cellStore->UnlockShareable();
  }

  /**
   * Dump trailer
   */
  cout << "timestamp " << cellStore->GetLogCutoffTime() << endl;
  KeyT *splitKey = cellStore->GetSplitKey();
  cout << "split key '" << *splitKey << "'" << endl;


  delete cellStore;

  return 0;
}
