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

extern "C" {
#include <string.h>
#include <sys/time.h>
}

#include "Common/FileUtils.h"
#include "Common/TestHarness.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/Schema.h"

#include "TestData.h"


using namespace hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: generateTestData <schemaFile>",
    "",
    "Generates test data to stdout, one line at a time, that is consumable by",
    "com.zvents.placer.Bigtable.RangeServer.TestSource.java",
    0
  };
}




int main(int argc, char **argv) {
  TestHarness harness("/tmp/generateTestData");
  TestData tdata(harness);
  Schema *schema;
  const char *buf;
  off_t len;
  struct timeval tval;
  uint64_t timestamp;
  uint32_t index;
  const char *rowKey;
  const char *qualifier;
  uint8_t family;
  const char *content, *ptr;
  std::string value;
  size_t cfMax;
  vector<std::string> cfNames;
  int modValue;
  
  if (argc != 2)
    Usage::DumpAndExit(usage);

  if ((buf = FileUtils::FileToBuffer(argv[1], &len)) == 0)
    exit(1);

  schema = Schema::NewInstance(buf, len, true);
  if (!schema->IsValid()) {
    LOG_VA_ERROR("Schema Parse Error: %s", schema->GetErrorString());
    exit(1);
  }
  delete [] buf;

  cfMax = schema->GetMaxColumnFamilyId();
  cfNames.resize(cfMax+1);

  list<Schema::AccessGroup *> *lgList = schema->GetAccessGroupList();
  for (list<Schema::AccessGroup *>::iterator iter = lgList->begin(); iter != lgList->end(); iter++) {
    for (list<Schema::ColumnFamily *>::iterator cfIter = (*iter)->columns.begin(); cfIter != (*iter)->columns.end(); cfIter++)
      cfNames[(*cfIter)->id] = (*cfIter)->name;
  }

  srand(1234);

  while (true) {

    // timestamp
    gettimeofday(&tval, 0);
    timestamp = ((uint64_t)tval.tv_sec * 1000000LL) + tval.tv_usec;

    // row key
    index = rand() % tdata.words.size();
    rowKey = tdata.words[index].get();

    index = rand();
    modValue = rand() % 49;
    if ((index % 49) == (size_t)modValue) {
      cout << timestamp << "\t" << rowKey << "\tDELETE" << endl;
      continue;
    }

    index = rand();
    family = (index % cfMax) + 1;
    
    index = rand() % tdata.urls.size();
    qualifier = tdata.urls[index].get();

    index = rand();
    modValue = rand() % 29;
    if ((index % 29) == 0) {
      cout << timestamp << "\t" << rowKey << "\t" << cfNames[family] << ":" << qualifier << "\tDELETE" << endl;
      continue;
    }

    index = rand() % tdata.content.size();
    content = tdata.content[index].get();
    
    if ((ptr = strchr(content, '\n')) != 0)
      value = std::string(content, ptr-content);
    else
      value = content;

    cout << timestamp << "\t" << rowKey << "\t" << cfNames[family] << ":" << qualifier << "\t" << value << endl;    

  }

  return 0;
}
