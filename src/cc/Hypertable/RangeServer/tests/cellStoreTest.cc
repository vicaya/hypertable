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

#include <algorithm>
#include <set>
#include <vector>


extern "C" {
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
}

#include "Common/ByteString.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/NumberStream.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorFactory.h"

#include "DfsBroker/Lib/Client.h"

#include "Hypertable/RangeServer/CellStoreScannerV0.h"
#include "Hypertable/RangeServer/CellStoreV0.h"
#include "Hypertable/RangeServer/CellCache.h"
#include "Hypertable/RangeServer/CellCacheScanner.h"
#include "Hypertable/RangeServer/FileBlockCache.h"
#include "Hypertable/RangeServer/Global.h"

#include "Hypertable/Lib/Key.h"
#include "Hypertable/Lib/TestData.h"


using namespace hypertable;
using namespace std;

namespace {
  const uint16_t DEFAULT_HDFSBROKER_PORT = 38546;

  const char *usage[] = {
    "usage: cellStoreTest [--golden]",
    "",
    "Validates CellStoreV0 class.  If --golden is supplied, a new golden",
    "output file 'cellStoreTest.golden' will be generated",
    0
  };
}




int main(int argc, char **argv) {
  Comm *comm;
  ConnectionManager *connManager;
  DfsBroker::Client *client;
  struct sockaddr_in addr;
  bool golden = false;

  ReactorFactory::Initialize(1);
  System::Initialize(argv[0]);

  TestHarness harness("/tmp/cellStoreTest");
  std::ostream &logStream = harness.GetLogStream();
  TestData tdata;
  NumberStream randstr("tests/random.dat");

  if (argc > 1) {
    if (!strcmp(argv[1], "--golden"))
      golden = true;
    else
      Usage::DumpAndExit(usage);
  }

  if (!tdata.Load("tests"))
    harness.DisplayErrorAndExit();

  logStream << "Content count = " << tdata.content.size() << endl;
  logStream << "Word count = " << tdata.words.size() << endl;
  logStream << "URL count = " << tdata.urls.size() << endl;

  Global::blockCache = new FileBlockCache(200000000LL);

  /**
   * Sort ordering test
   */

  typedef set<ByteString32T *, ltByteString32> KeySetT;

  KeySetT keys;
  ByteString32T  *key;
  ByteString32T *value;
  uint32_t index;
  uint8_t family;
  const char *qualifier;
  const char *row;
  int64_t timestamp;

  keys.clear();

  for (size_t i=0; i<10000; i++) {

    family = randstr.getInt() % 4;
    timestamp = randstr.getInt() % 20;

    index = randstr.getInt() % 10;
    if (index & 1)
      qualifier = tdata.urls[index].get();
    else
      qualifier = 0;

    keys.insert( CreateKey(FLAG_INSERT, tdata.urls[0].get(), family, qualifier, timestamp) );
  }

  for (KeySetT::iterator iter = keys.begin(); iter != keys.end(); iter++)
    logStream << Key(*iter) << endl;


  /**
   *  Write/Scan test
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

  comm = new Comm();
  connManager = new ConnectionManager(comm);

  client = new DfsBroker::Client(connManager, addr, 20);
  if (!client->WaitForConnection(10))
    harness.DisplayErrorAndExit();

  CellStorePtr cellStorePtr( new CellStoreV0(client) );
  CellCachePtr cellCachePtr( new CellCache() );

  if (cellStorePtr->Create("/cellStore.tmp") != 0)
    harness.DisplayErrorAndExit();

  typedef std::map<ByteString32T *, ByteString32T *, ltByteString32> KeyValueMapT;

  KeyValueMapT  kvMap;
  kvMap.clear();
  ByteString32T *startKey, *endKey;

  for (size_t i=0; i<20000; i++) {

    row = tdata.urls[i].get();
    family = randstr.getInt() % 4;
    timestamp = randstr.getInt();

    index = randstr.getInt() % tdata.urls.size();

    if (index & 1)
      qualifier = tdata.urls[index].get();
    else
      qualifier = 0;

    key = CreateKey(FLAG_INSERT, row, family, qualifier, timestamp);

    index = randstr.getInt() % tdata.content.size();
    value = (ByteString32T *)new uint8_t [ sizeof(int32_t) + strlen(tdata.content[index].get()) ];
    value->len = strlen(tdata.content[index].get());
    memcpy(value->data, tdata.content[index].get(), value->len);

    kvMap.insert( KeyValueMapT::value_type(key, value) );
  }

  KeyValueMapT::iterator iter = kvMap.begin();
  for (size_t i=0; i<15000; i++) {
    if (i == 6000)
      startKey = (*iter).first;
    iter++;
  }
  endKey = (*iter).first;

  logStream << "Range Start: " << Key(startKey) << endl;
  logStream << "Range End: " << Key(endKey) << endl;

  for (KeyValueMapT::iterator iter = kvMap.begin(); iter != kvMap.end(); iter++) {
    cellStorePtr->Add((*iter).first, (*iter).second);
    cellCachePtr->Add((*iter).first, (*iter).second);
  }

  if (cellStorePtr->Finalize(1234567) != 0) {
    cout << "Problem finalizing CellStore '/cellStore.tmp'" << endl;
    exit(1);
  }

  CellListScanner *scanner = 0;

  /**
   * Scan through newly created table dumping out keys
   */
  ScanContextPtr scanContextPtr;

  scanContextPtr.reset( new ScanContext(ScanContext::END_OF_TIME, 0) ); 
  scanContextPtr->startKeyPtr.reset(CreateCopy(startKey));
  scanContextPtr->endKeyPtr.reset(CreateCopy(endKey));

  scanner = cellStorePtr->CreateScanner(scanContextPtr);

  char outfileA[32];
  sprintf(outfileA, "/tmp/cellStoreTest-1-%d", getpid());
  ofstream outstreamA(outfileA);

  while (scanner->Get(&key, &value)) {
    outstreamA << *key << endl;
    scanner->Forward();
  }
  outstreamA.close();

  delete scanner;

  /**
   * Scan through re-opened table dumping out keys
   */

  cellStorePtr = new CellStoreV0(client);

  if (cellStorePtr->Open("/cellStore.tmp", 0, 0) != 0)
    harness.DisplayErrorAndExit();

  if (cellStorePtr->LoadIndex() != 0)
    harness.DisplayErrorAndExit();    

  scanner = cellStorePtr->CreateScanner(scanContextPtr);
  /**
  scanner->RestrictRange(startKey, endKey);
  scanner->Reset();
  **/

  char outfileB[32];
  sprintf(outfileB, "/tmp/cellStoreTest-2-%d", getpid());
  ofstream outstreamB(outfileB);

  while (scanner->Get(&key, &value)) {
    outstreamB << *key << endl;
    scanner->Forward();
  }
  outstreamB.close();

  delete scanner;
  
  string command = (string)"diff " + outfileA + " " + outfileB;
  if (system(command.c_str())) {
    cerr << "Error: " << command << endl;
    exit(1);
  }

  scanner = cellCachePtr->CreateScanner(scanContextPtr);

  char outfileC[32];
  strcpy(outfileC, "/tmp/cellStoreTest-3-XXXXXX");
  if (mkstemp(outfileC) == 0) {
    LOG_VA_ERROR("mkstemp(\"%s\") failed - %s", outfileC, strerror(errno));
    exit(1);
  }
  ofstream outstreamC(outfileC);

  while (scanner->Get(&key, &value)) {
    outstreamC << *key << endl;
    scanner->Forward();
  }
  outstreamC.close();

  delete scanner;

  command = (string)"diff " + outfileA + " " + outfileC;
  if (system(command.c_str())) {
    cerr << "Error: " << command << endl;
    exit(1);
  }

  unlink(outfileA);
  unlink(outfileB);
  unlink(outfileC);

  if (!golden)
    harness.ValidateAndExit("tests/cellStoreTest.golden");

  harness.RegenerateGoldenFile("tests/cellStoreTest.golden");

  return 0;
}
