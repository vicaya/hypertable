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

using namespace Hypertable;
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

  System::initialize(argv[0]);

  if (argc < 2)
    Usage::dump_and_exit(usage);

  metadata = Metadata::new_instance(argv[1]);

  //metadata->display();

  RangeInfoPtr rangeInfoPtr;

  string tableName = "UserSession";
  string startRow = "juggernaut";
  string endRow = "";
  
  if ((error = metadata->get_range_info(tableName, endRow, rangeInfoPtr)) != Error::OK) {
    LOG_VA_ERROR("Problem locating range (table='%s' endRow='%s') - %s",
		 tableName.c_str(), endRow.c_str(), Error::get_text(error));
    exit(1);
  }

  str = "palomar";
  rangeInfoPtr->set_end_row(str);

  metadata->add_range_info(rangeInfoPtr);

  rangeInfoPtr = new RangeInfo();
  RangeInfo *newRange = rangeInfoPtr.get();

  str = "UserSession";
  newRange->set_table_name(str);

  str = "palomar";
  newRange->set_start_row(str);

  str = "";
  newRange->set_end_row(str);

  str = "/bigtable/tables/UserSession/default/abcdef0123abcdef0123abcd/ss2349";
  newRange->add_cell_store(str);
  
  str = "/bigtable/tables/UserSession/default/abcdef0123abcdef0123abcd/ss2350";
  newRange->add_cell_store(str);

  metadata->add_range_info(rangeInfoPtr);

  metadata->sync("/tmp/metadata.xml");

  metadata->display();

  delete metadata;

  system("cat /tmp/metadata.xml");

  unlink("/tmp/metadata.xml");

  return 0;
}
