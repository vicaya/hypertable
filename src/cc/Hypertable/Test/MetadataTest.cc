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
#include <iostream>

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "Metadata.h"

using namespace hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: MetaDataTest <fname>",
    "",
    "Parses and validates metadata file <fname>",
    0
  };
}


int main(int argc, char **argv) {
  Metadata *metadata;
  int error;
  string str;

  System::Initialize(argv[0]);

  if (argc < 2)
    Usage::DumpAndExit(usage);

  metadata = Metadata::NewInstance(argv[1]);

  //metadata->Display();

  RangeInfoPtr rangeInfoPtr;

  string tableName = "UserSession";
  string startRow = "juggernaut";
  string endRow = "";
  
  if ((error = metadata->GetRangeInfo(tableName, startRow, endRow, rangeInfoPtr)) != Error::OK) {
    LOG_VA_ERROR("Problem locating range (table='%s' startRow='%s' endRow='%s') - %s",
		 tableName.c_str(), startRow.c_str(), endRow.c_str(), Error::GetText(error));
    exit(1);
  }

  str = "palomar";
  rangeInfoPtr->SetEndRow(str);

  metadata->AddRangeInfo(rangeInfoPtr);

  rangeInfoPtr.reset(new RangeInfo());
  RangeInfo *newRange = rangeInfoPtr.get();

  str = "UserSession";
  newRange->SetTableName(str);

  str = "palomar";
  newRange->SetStartRow(str);

  str = "";
  newRange->SetEndRow(str);

  str = "/bigtable/tables/UserSession/default/abcdef0123abcdef0123abcd/ss2349";
  newRange->AddCellStore(str);
  
  str = "/bigtable/tables/UserSession/default/abcdef0123abcdef0123abcd/ss2350";
  newRange->AddCellStore(str);

  metadata->AddRangeInfo(rangeInfoPtr);

  metadata->Sync("/tmp/metadata.xml");

  metadata->Display();

  delete metadata;

  system("cat /tmp/metadata.xml");

  unlink("/tmp/metadata.xml");

  return 0;
}
