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
  
  if ((error = metadata->GetRangeInfo(tableName, endRow, rangeInfoPtr)) != Error::OK) {
    LOG_VA_ERROR("Problem locating range (table='%s' endRow='%s') - %s",
		 tableName.c_str(), endRow.c_str(), Error::GetText(error));
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
