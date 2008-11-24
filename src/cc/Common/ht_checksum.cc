/**
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

#include <iostream>

#include "Checksum.h"
#include "FileUtils.h"

using namespace Hypertable;
using namespace std;

namespace {

  const char *usage_str = 
    "\n" \
    "usage: java org.hypertable.Common.Checksum <algorithm> <file>\n" \
    "\n" \
    "This program runs the checksum algorithm, <algorithm>, over the bytes\n" \
    "of the input file <file> and displays the resulting checksum as a\n" \
    "signed integer.\n" \
    "\n" \
    "Supported Algorithms:\n" \
    "\n" \
    "  fletcher32\n" \
    "\n";

}

/**
 *
 */
int main(int argc, char **argv) {

  if (argc != 3) {
    cout << usage_str << endl;
    exit(1);
  }

  if (!strcmp(argv[1], "fletcher32")) {
    off_t len;
    char *data = FileUtils::file_to_buffer(argv[2], &len);
    int32_t checksum = fletcher32(data, len);
    cout << checksum << endl;
  }
  else {
    cout << usage_str << endl;
    exit(1);
  }

  return 0;
}
