/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
 *
 * Hypertable is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include <cstdlib>
#include <iostream>
#include <vector>

extern "C" {
#include <errno.h>
#include <fcntl.h>
}

#include "Common/String.h"

#include "Hypertable/Lib/KeySpec.h"
#include "Hypertable/Lib/LoadDataSource.h"
#include "Hypertable/Lib/LoadDataSourceFactory.h"
#include "DfsBroker/Lib/Client.h"

using namespace Hypertable;
using namespace std;

int main(int argc, char **argv) {
  LoadDataSourcePtr lds;
  KeySpec key;
  uint8_t *value;
  uint32_t value_len;
  int fd;
  std::vector<String> key_columns;
  DfsBroker::ClientPtr null_dfs_client;

  vector<String> testnames;
  testnames.push_back("loadDataSourceTest");
  testnames.push_back("loadDataSourceTest-header");
  testnames.push_back("loadDataSourceTest-qualified-header");

  for(size_t i = 0; i < testnames.size(); i++) {
    String output_fn = testnames[i] + ".output";

    if ((fd = open(output_fn.c_str(), O_WRONLY|O_CREAT|O_TRUNC, 0644)) < 0) {
      perror("open");
      return 1;
    }

    close(2);
    dup(fd);

    key_columns.clear();
    String dat_fn = testnames[i] + ".dat";
    lds = LoadDataSourceFactory::create(null_dfs_client,
                                        dat_fn.c_str(), LOCAL_FILE, "", LOCAL_FILE,
                                        key_columns, "");

    while (lds->next(0, &key, &value, &value_len, 0)) {
      cerr << "row=" << (const char *)key.row
           << " column_family=" << key.column_family;
      if (key.column_qualifier_len > 0)
        cerr << " column_qualifier=" << (const char *)key.column_qualifier;
      cerr << " value=" << (const char *)value << endl;
    }

    String golden_fn = testnames[i] + ".golden";
    String sys_cmd = "diff " + output_fn + " " + golden_fn;
    if (system(sys_cmd.c_str()) != 0)
      return 1;
  }

  return 0;
}
