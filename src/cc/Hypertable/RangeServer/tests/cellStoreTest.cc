/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
#include "AsyncComm/ReactorFactory.h"

#include "Hypertable/HdfsClient/HdfsClient.h"
#include "Hypertable/RangeServer/CellStoreV0.h"
#include "Hypertable/RangeServer/CellCache.h"

#include "Hypertable/RangeServer/FileBlockCache.h"
#include "Hypertable/RangeServer/Global.h"
#include "Hypertable/RangeServer/Key.h"

#include "TestData.h"


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
  HdfsClient *client;
  struct sockaddr_in addr;
  bool golden = false;

  ReactorFactory::Initialize(1);
  System::Initialize(argv[0]);

  TestHarness harness("/tmp/cellStoreTest");
  std::ostream &logStream = harness.GetLogStream();
  TestData tdata(harness);
  NumberStream randstr("tests/random.dat");

  if (argc > 1) {
    if (!strcmp(argv[1], "--golden"))
      golden = true;
    else
      Usage::DumpAndExit(usage);
  }

  logStream << "Content count = " << tdata.content.size() << endl;
  logStream << "Word count = " << tdata.words.size() << endl;
  logStream << "URL count = " << tdata.urls.size() << endl;

  Global::blockCache = new FileBlockCache(200000000LL);

  /**
   * Sort ordering test
   */

  typedef set<KeyPtr, ltKeyPtr> KeySetT;

  KeySetT keys;
  KeyT  *key;
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

    keys.insert( KeyPtr(CreateKey(FLAG_INSERT, tdata.urls[0].get(), family, qualifier, timestamp)) );
  }

  for (KeySetT::iterator iter = keys.begin(); iter != keys.end(); iter++) {
    key = (KeyT *)(*iter).get();
    logStream << *key << endl;
  }

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

  comm = new Comm(0);

  client = new HdfsClient(comm, addr, 20);
  if (!client->WaitForConnection(10))
    harness.DisplayErrorAndExit();

  CellStoreV0 *cellStore = new CellStoreV0(client);
  CellCache *memtable = new CellCache();

  if (cellStore->Create("/cellStore.tmp") != 0)
    harness.DisplayErrorAndExit();

  typedef std::map<KeyPtr, ByteString32Ptr, ltKeyPtr> KeyValueMapT;

  KeyValueMapT  kvMap;
  kvMap.clear();
  KeyPtr startKeyPtr;
  KeyPtr endKeyPtr;
  KeyT *startKey, *endKey;

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

    kvMap.insert( KeyValueMapT::value_type(KeyPtr(key), ByteString32Ptr(value)) );
  }

  KeyValueMapT::iterator iter = kvMap.begin();
  for (size_t i=0; i<15000; i++) {
    if (i == 6000)
      startKeyPtr = (*iter).first;
    iter++;
  }
  endKeyPtr = (*iter).first;

  startKey = (KeyT *)startKeyPtr.get();
  endKey = (KeyT *)endKeyPtr.get();

  logStream << "Range Start: " << *(KeyT *)startKeyPtr.get() << endl;
  logStream << "Range End: " << *(KeyT *)endKeyPtr.get() << endl;

  for (KeyValueMapT::iterator iter = kvMap.begin(); iter != kvMap.end(); iter++) {
    key = (KeyT *)(*iter).first.get();
    value = (ByteString32T *)(*iter).second.get();
    cellStore->Add(key, value);
    memtable->Add(key, value);
  }

  /**
  vector<string> replacedFiles;
  string fname = "/bt/tables/Webtable/default/abcdef0123abcdef0123abcd/ss2345";
  replacedFiles.push_back(fname);
  fname = "/bt/tables/Webtable/default/abcdef0123abcdef0123abcd/ss7654";
  replacedFiles.push_back(fname);
  **/

  if (cellStore->Finalize(1234567) != 0) {
    cout << "Problem finalizing CellStore '/cellStore.tmp'" << endl;
    exit(1);
  }

  CellListScanner *scanner = 0;

  /**
   * Scan through newly created table dumping out keys
   */
  cellStore->LockShareable();
  scanner = cellStore->CreateScanner();
  scanner->RestrictRange(startKey, endKey);
  scanner->Reset();

  char outfileA[32];
  strcpy(outfileA, "/tmp/cellStoreTest-1-XXXXXX");
  if (mktemp(outfileA) == 0) {
    LOG_VA_ERROR("mktemp(\"%s\") failed - %s", outfileA, strerror(errno));
    exit(1);
  }
  ofstream outstreamA(outfileA);

  while (scanner->Get(&key, &value)) {
    outstreamA << *key << endl;
    scanner->Forward();
  }
  outstreamA.close();

  delete scanner;
  cellStore->UnlockShareable();
  delete cellStore;

  /**
   * Scan through re-opened table dumping out keys
   */

  cellStore = new CellStoreV0(client);

  if (cellStore->Open("/cellStore.tmp", 0, 0) != 0)
    harness.DisplayErrorAndExit();

  if (cellStore->LoadIndex() != 0)
    harness.DisplayErrorAndExit();    

  cellStore->LockShareable();
  scanner = cellStore->CreateScanner();
  scanner->RestrictRange(startKey, endKey);
  scanner->Reset();

  char outfileB[32];
  strcpy(outfileB, "/tmp/cellStoreTest-2-XXXXXX");
  if (mktemp(outfileB) == 0) {
    LOG_VA_ERROR("mktemp(\"%s\") failed - %s", outfileB, strerror(errno));
    exit(1);
  }
  ofstream outstreamB(outfileB);

  while (scanner->Get(&key, &value)) {
    outstreamB << *key << endl;
    scanner->Forward();
  }
  outstreamB.close();

  delete scanner;
  cellStore->UnlockShareable();
  delete cellStore;
  
  string command = (string)"diff " + outfileA + " " + outfileB;
  if (system(command.c_str())) {
    cerr << "Error: " << command << endl;
    exit(1);
  }

  memtable->LockShareable();
  scanner = memtable->CreateScanner();
  scanner->RestrictRange(startKey, endKey);
  scanner->Reset();

  char outfileC[32];
  strcpy(outfileC, "/tmp/cellStoreTest-3-XXXXXX");
  if (mktemp(outfileC) == 0) {
    LOG_VA_ERROR("mktemp(\"%s\") failed - %s", outfileC, strerror(errno));
    exit(1);
  }
  ofstream outstreamC(outfileC);

  while (scanner->Get(&key, &value)) {
    outstreamC << *key << endl;
    scanner->Forward();
  }
  outstreamC.close();

  delete scanner;
  memtable->UnlockShareable();
  delete memtable;

  command = (string)"diff " + outfileA + " " + outfileC;
  if (system(command.c_str())) {
    cerr << "Error: " << command << endl;
    exit(1);
  }

  /**
  unlink(outfileA);
  unlink(outfileB);
  unlink(outfileC);
  **/

  if (!golden)
    harness.ValidateAndExit("tests/cellStoreTest.golden");

  harness.RegenerateGoldenFile("tests/cellStoreTest.golden");

  return 0;
}
