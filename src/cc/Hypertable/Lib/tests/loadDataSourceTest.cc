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

#include <cstdlib>
#include <iostream>

extern "C" {
#include <errno.h>
#include <fcntl.h>
}


#include "Hypertable/Lib/KeySpec.h"
#include "Hypertable/Lib/LoadDataSource.h"

using namespace Hypertable;
using namespace std;

int main(int argc, char **argv) {
  LoadDataSource  *lds;
  uint64_t timestamp;
  KeySpec key;
  uint8_t *value;
  uint32_t value_len;
  int fd;

  if ((fd = open("loadDataSourceTest.output", O_WRONLY|O_CREAT|O_TRUNC, 0644)) < 0) {
    perror("open");
    return 1;
  }

  close(2);
  dup(fd);

  lds = new LoadDataSource("loadDataSourceTest.dat", "", "");

  while (lds->next(0, &timestamp, &key, &value, &value_len, 0)) {
    cerr << "row=" << (const char *)key.row << " column_family=" << key.column_family;
    if (key.column_qualifier_len > 0)
      cerr << " column_qualifier=" << (const char *)key.column_qualifier;
    cerr << " value=" << (const char *)value << endl;
  }

  delete lds;

  close(2);
  dup(1);

  if (system("diff loadDataSourceTest.output loadDataSourceTest.golden") != 0)
    return 1;

  return 0;
}
