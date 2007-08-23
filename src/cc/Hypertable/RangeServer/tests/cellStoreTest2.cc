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

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/ReactorFactory.h"

#include "Common/ByteString.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/TestHarness.h"
#include "Common/Usage.h"

#include "HdfsClient/HdfsClient.h"
#include "Hypertable/Lib/Schema.h"
#include "Hypertable/RangeServer/CellCache.h"
#include "Hypertable/RangeServer/CellCacheScanner.h"
#include "Hypertable/RangeServer/MergeScanner.h"
#include "Hypertable/RangeServer/CellStoreScannerV0.h"
#include "Hypertable/RangeServer/CellStoreV0.h"

#include "TestSource.h"

#include "Hypertable/RangeServer/FileBlockCache.h"
#include "Hypertable/RangeServer/Global.h"


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
  ConnectionManager *connManager;
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

  comm = new Comm();
  connManager = new ConnectionManager(comm);

  client = new HdfsClient(connManager, addr, 20);
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
    ByteString32T *key;
    ByteString32T *value;
    CellStorePtr cellStorePtr[4];
    CellCachePtr cellCachePtr;
    //vector<string> files;
    MergeScanner *mscanner, *mscannerA, *mscannerB;

    ScanContextPtr scanContextPtr( new ScanContext(ScanContext::END_OF_TIME, 0) ); 

    if ((schemaData = FileUtils::FileToBuffer("tests/Test.xml", &len)) == 0)
      return false;
    schema = Schema::NewInstance(schemaData, len, true);
    if (!schema->IsValid()) {
      LOG_VA_ERROR("Schema Parse Error: %s", schema->GetErrorString());
      return false;
    }

    for (size_t i=0; i<4; i++)
      cellStorePtr[i] = new CellStoreV0(client);

    if (cellStorePtr[0]->Create("/cellStore0.tmp") != 0)
      return false;

    if (cellStorePtr[1]->Create("/cellStore1.tmp") != 0)
      return false;

    if (cellStorePtr[2]->Create("/cellStore2.tmp") != 0)
      return false;

    if (cellStorePtr[3]->Create("/cellStore3.tmp") != 0)
      return false;

    cellCachePtr = new CellCache();

    std::string inputFile = "tests/testdata.txt";

    TestSource *inputSource = new TestSource(inputFile, schema);

    typedef std::map<ByteString32T *, ByteString32T *, ltByteString32> KeyValueMapT;

    KeyValueMapT  kvMap;
    Key keyComps;
    kvMap.clear();

    for (size_t i=0; inputSource->Next(&key, &value); i++) {
      kvMap.insert( KeyValueMapT::value_type(CreateCopy(key), CreateCopy(value)) );  // do we need to make the copy?
    }

    size_t i=0;
    for (KeyValueMapT::iterator iter = kvMap.begin(); iter != kvMap.end(); iter++) {
    
      if (!keyComps.load((*iter).first)) {
	LOG_ERROR("Problem parsing key!!");
	return 1;
      }

      cellCachePtr->Add((*iter).first, (*iter).second);

      if (keyComps.flag == FLAG_INSERT) {
	if (cellStorePtr[i%4]->Add((*iter).first, (*iter).second) != Error::OK)
	  return false;
      }
      else {
	for (size_t j=0; j<4; j++) {
	  if (cellStorePtr[j]->Add((*iter).first, (*iter).second) != Error::OK)
	    return false;
	}
      }
      i++;
    }

    for (size_t i=0; i<4; i++)
      cellStorePtr[i]->Finalize(0);

    char outfileA[64];
    strcpy(outfileA, "/tmp/cellStoreTest1-cellCache-XXXXXX");
    if (mktemp(outfileA) == 0) {
      LOG_VA_ERROR("mktemp(\"%s\") failed - %s", outfileA, strerror(errno));
      exit(1);
    }
    ofstream outstreamA(outfileA);

    mscanner = new MergeScanner(scanContextPtr, !suppressDeleted);
    mscanner->AddScanner( cellCachePtr->CreateScanner(scanContextPtr) );

    while (mscanner->Get(&key, &value)) {
      outstreamA << Key(key) << " " << *value << endl;
      mscanner->Forward();
    }

    char outfileB[64];
    strcpy(outfileB, "/tmp/cellStoreTest1-merge-XXXXXX");
    if (mktemp(outfileB) == 0) {
      LOG_VA_ERROR("mktemp(\"%s\") failed - %s", outfileB, strerror(errno));
      exit(1);
    }
    ofstream outstreamB(outfileB);

    mscanner = new MergeScanner(scanContextPtr, !suppressDeleted);

    mscannerA = new MergeScanner(scanContextPtr, !suppressDeleted);
    mscannerA->AddScanner( cellStorePtr[0]->CreateScanner(scanContextPtr) );
    mscannerA->AddScanner( cellStorePtr[1]->CreateScanner(scanContextPtr) );
    mscanner->AddScanner(mscannerA);

    mscannerB = new MergeScanner(scanContextPtr, !suppressDeleted);
    mscannerB->AddScanner( cellStorePtr[2]->CreateScanner(scanContextPtr) );
    mscannerB->AddScanner( cellStorePtr[3]->CreateScanner(scanContextPtr) );
    mscanner->AddScanner(mscannerB);

    while (mscanner->Get(&key, &value)) {
      outstreamB << Key(key) << " " << *value << endl;
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
