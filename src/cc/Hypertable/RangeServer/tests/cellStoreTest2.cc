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

#include "AsyncComm/Comm.h"
#include "AsyncComm/ReactorFactory.h"

#include "Common/ByteString.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/TestHarness.h"
#include "Common/Usage.h"

#include "HdfsClient/HdfsClient.h"
#include "Hypertable/Schema.h"
#include "Hypertable/RangeServer/CellCache.h"
#include "Hypertable/RangeServer/CellCacheScanner.h"
#include "Hypertable/RangeServer/MergeScanner.h"
#include "Hypertable/RangeServer/CellStoreScannerV0.h"
#include "Hypertable/RangeServer/CellStoreV0.h"

#include "TestSource.h"

#include "Hypertable/RangeServer/FileBlockCache.h"
#include "Hypertable/RangeServer/Global.h"
#include "Hypertable/RangeServer/Key.h"


using namespace hypertable;
using namespace std;


namespace {
  const uint16_t DEFAULT_HDFSBROKER_PORT = 38546;
  const char *usage[] = {
    "usage: cellStoreTest2 [--golden]",
    "",
    "Validates CellStoreV0 class.  If --golden is supplied, a new golden",
    "output file 'cellStoreTest2.golden' will be generated",
    0
  };
  bool RunMergeTest(Comm *comm, HdfsClient *client, bool suppressDeleted);
}



int main(int argc, char **argv) {
  Comm *comm;
  HdfsClient *client;
  bool golden = false;
  struct sockaddr_in addr;
  TestHarness harness("/tmp/cellStoreTest2");

  ReactorFactory::Initialize(1);
  System::Initialize(argv[0]);

  if (argc > 1) {
    if (!strcmp(argv[1], "--golden"))
      golden = true;
    else
      Usage::DumpAndExit(usage);
  }

  Global::blockCache = new FileBlockCache(200000000LL);

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

  if (!RunMergeTest(comm, client, true))
    harness.DisplayErrorAndExit();

  return 0;
}



namespace {

  bool RunMergeTest(Comm *comm, HdfsClient *client, bool suppressDeleted) {
    const char *schemaData;
    off_t len;
    Schema *schema;
    KeyT *key;
    ByteString32T *value;
    CellStorePtr cellStorePtr[4];
    CellCachePtr cellCachePtr;
    //vector<string> files;
    MergeScanner *mscanner, *mscannerA, *mscannerB;

    if ((schemaData = FileUtils::FileToBuffer("tests/Test.xml", &len)) == 0)
      return false;
    schema = Schema::NewInstance(schemaData, len, true);
    if (!schema->IsValid()) {
      LOG_VA_ERROR("Schema Parse Error: %s", schema->GetErrorString());
      return false;
    }

    for (size_t i=0; i<4; i++)
      cellStorePtr[i].reset( new CellStoreV0(client) );

    if (cellStorePtr[0]->Create("/cellStore0.tmp") != 0)
      return false;

    if (cellStorePtr[1]->Create("/cellStore1.tmp") != 0)
      return false;

    if (cellStorePtr[2]->Create("/cellStore2.tmp") != 0)
      return false;

    if (cellStorePtr[3]->Create("/cellStore3.tmp") != 0)
      return false;

    cellCachePtr.reset( new CellCache() );

    std::string inputFile = "tests/testdata.txt";

    TestSource *inputSource = new TestSource(inputFile, schema);

    typedef std::map<KeyPtr, ByteString32Ptr, ltKeyPtr> KeyValueMapT;

    KeyValueMapT  kvMap;
    KeyComponentsT keyComps;
    kvMap.clear();

    for (size_t i=0; inputSource->Next(&key, &value); i++) {
      kvMap.insert( KeyValueMapT::value_type(KeyPtr(CreateCopy(key)), ByteString32Ptr(CreateCopy(value))) );
    }

    size_t i=0;
    for (KeyValueMapT::iterator iter = kvMap.begin(); iter != kvMap.end(); iter++) {
    
      if (!Load((*iter).first.get(), keyComps)) {
	LOG_ERROR("Problem parsing key!!");
	return 1;
      }

      cellCachePtr->Add((*iter).first.get(), (*iter).second.get());

      if (keyComps.flag == FLAG_INSERT) {
	if (cellStorePtr[i%4]->Add((*iter).first.get(), (*iter).second.get()) != Error::OK)
	  return false;
      }
      else {
	for (size_t j=0; j<4; j++) {
	  if (cellStorePtr[j]->Add((*iter).first.get(), (*iter).second.get()) != Error::OK)
	    return false;
	}
      }
      i++;
    }

    //files.push_back("foo");
    //files.push_back("bar");

    for (size_t i=0; i<4; i++)
      cellStorePtr[i]->Finalize(0);

    char outfileA[64];
    strcpy(outfileA, "/tmp/cellStoreTest1-cellCache-XXXXXX");
    if (mktemp(outfileA) == 0) {
      LOG_VA_ERROR("mktemp(\"%s\") failed - %s", outfileA, strerror(errno));
      exit(1);
    }
    ofstream outstreamA(outfileA);

    mscanner = new MergeScanner(suppressDeleted);
    mscanner->AddScanner( new CellCacheScanner(cellCachePtr) );
    mscanner->Reset();

    //CellSequenceScanner *scanner = cellCachePtr->CreateScanner(suppressDeleted);
    //scanner->Reset();

    while (mscanner->Get(&key, &value)) {
      outstreamA << *key << " " << *value << endl;
      //scanner->Forward();
      mscanner->Forward();
    }

    //delete scanner;
    //delete cellCache;

    char outfileB[64];
    strcpy(outfileB, "/tmp/cellStoreTest1-merge-XXXXXX");
    if (mktemp(outfileB) == 0) {
      LOG_VA_ERROR("mktemp(\"%s\") failed - %s", outfileB, strerror(errno));
      exit(1);
    }
    ofstream outstreamB(outfileB);

    mscanner = new MergeScanner(suppressDeleted);

    mscannerA = new MergeScanner(suppressDeleted);
    mscannerA->AddScanner( new CellStoreScannerV0(cellStorePtr[0]) );
    mscannerA->AddScanner( new CellStoreScannerV0(cellStorePtr[1]) );
    mscanner->AddScanner(mscannerA);

    mscannerB = new MergeScanner(suppressDeleted);
    mscannerB->AddScanner( new CellStoreScannerV0(cellStorePtr[2]) );
    mscannerB->AddScanner( new CellStoreScannerV0(cellStorePtr[3]) );
    mscanner->AddScanner(mscannerB);

    mscanner->Reset();

    while (mscanner->Get(&key, &value)) {
      outstreamB << *key << " " << *value << endl;
      mscanner->Forward();
    }

    string command = (string)"diff " + outfileA + " " + outfileB;
    if (system(command.c_str())) {
      cerr << "Error: " << command << endl;
      exit(1);
    }

    unlink(outfileA);
    unlink(outfileB);

    return true;
    
  }

}
