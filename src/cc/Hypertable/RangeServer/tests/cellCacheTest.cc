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

#include <cassert>
#include <iostream>

extern "C" {
#include <limits.h>
#include <stdlib.h>
#include <string.h>
}

#include "Common/ByteString.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/TestHarness.h"
#include "Common/Usage.h"

#include "Hypertable/RangeServer/Key.h"
#include "Hypertable/RangeServer/CellCache.h"
#include "Hypertable/RangeServer/CellCacheScanner.h"

using namespace hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: cellCacheTest",
    "",
    "This program ...",
    0
  };
}


void LoadCell(char *line, KeyPtr &keyPtr, ByteString32Ptr &valuePtr) {
  char *base = line;
  char *ptr;
  char *rowKey = line;
  char *qualifier;
  uint8_t   family = 0;
  uint64_t  timestamp = 0;
  uint32_t  flag = FLAG_INSERT;

  // row key
  ptr = strchr(base, '\t');
  *ptr = 0;

  // column family
  base = ++ptr;
  ptr = strchr(base, '\t');
  *ptr = 0;
  if (*base != 0)
    family = (uint8_t)atoi(base);

  // column qualifier
  base = ++ptr;
  qualifier = base;
  ptr = strchr(base, '\t');
  *ptr = 0;

  // timestamp
  base = ++ptr;
  ptr = strchr(base, '\t');
  *ptr = 0;
  timestamp = (uint64_t)strtoll(base, 0, 0);

  // value
  base = ++ptr;
  if (*base != 0) {
    if (!strcmp(base, "ROW_DELETE")) {
      flag = FLAG_DELETE_ROW;
      valuePtr.reset(0);
    }
    else if (!strcmp(base, "ROW_COLUMN_DELETE")) {
      flag = FLAG_DELETE_CELL;
      valuePtr.reset(0);
    }
    else {
      ByteString32T *value = (ByteString32T *)new uint8_t [ sizeof(int32_t) + strlen(base) ];
      value->len = strlen(base);
      strcpy((char *)value->data, base);
      valuePtr.reset(value);
    }
  }

  keyPtr.reset( CreateKey(flag, rowKey, family, qualifier, timestamp) );
}




int main(int argc, char **argv) {
  char *cellData, *line, *last;
  off_t len;
  CellCachePtr cellCachePtr;
  TestHarness harness("/tmp/cellCacheTest");
  CellListScanner  *scanner;
  KeyT             *key;
  ByteString32T    *value;
  KeyPtr           keyPtr;
  ByteString32Ptr  valuePtr;
  
  if (argc != 1)
    Usage::DumpAndExit(usage);

  if ((cellData = FileUtils::FileToBuffer("tests/cellCacheTestData.txt", &len)) == 0)
    harness.DisplayErrorAndExit();

  cellCachePtr.reset( new CellCache() );

  for (line = strtok_r(cellData, "\n\r", &last); line; line = strtok_r(0, "\n\r", &last)) {
    LoadCell(line, keyPtr, valuePtr);
    cellCachePtr->Add(keyPtr.get(), valuePtr.get());
    //cout << cell << endl << flush;
    //cells.push_back(cell);
    //cout << cell << endl;
  }

  cout << endl;

  scanner = new CellCacheScanner( cellCachePtr );
  scanner->Reset();
  while (scanner->Get(&key, &value)) {
    cout << *key << " " << *value << endl << flush;
    scanner->Forward();
  }

  // Load up 3 cellCaches with data

  // Delete some rows

  // Add some of the same rows back in

  // Delete some cells

  // Add some of the same cells back in

  // Create iterators on the three tables

  // Create a merge iterator

  // Iterate through the merged view of the tables



  /*
  boost::thread  *thread1, *thread2;
  struct sockaddr_in addr;


  System::Initialize(argv[0]);
  ReactorFactory::Initialize(1);

  memset(&addr, 0, sizeof(struct sockaddr_in));
  {
    struct hostent *he = gethostbyname("localhost");
    if (he == 0) {
      herror("gethostbyname()");
      return 1;
    }
    memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
  }
  addr.sin_family = AF_INET;
  addr.sin_port = htons(DEFAULT_PORT);

  Global::comm = new Comm(0);
  Global::client = new Hdfs::Client(Global::comm, addr, 15);
  Global::protocol = new Hdfs::Protocol();

  if (!Global::client->WaitForConnection(15)) {
    LOG_ERROR("Unable to connect to HDFS");
    return 1;
  }

  HdfsTestThreadFunction threadFunc(addr, "/usr/share/dict/words");

  threadFunc.SetHdfsFile("/RegressionTests/hdfs/output.a");
  threadFunc.SetOutputFile("tests/output.a");
  thread1 = new boost::thread(threadFunc);

  threadFunc.SetHdfsFile("/RegressionTests/hdfs/output.b");
  threadFunc.SetOutputFile("tests/output.b");
  thread2 = new boost::thread(threadFunc);

  thread1->join();
  thread2->join();

  if (system("diff /usr/share/dict/words tests/output.a"))
    return 1;

  if (system("diff tests/output.a tests/output.b"))
    return 1;
  */
  return 0;
}
