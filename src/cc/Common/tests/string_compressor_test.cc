/** -*- C++ -*-
 * Copyright (C) 2011  Hypertable, Inc.
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include "Common/Compat.h"

#include <vector>
#include <iostream>

#include "Common/StringCompressorPrefix.h"
#include "Common/StringDecompressorPrefix.h"
#include "Common/Serialization.h"

using namespace std;
using namespace Hypertable;

int main(int argc, char *argv[]) {
  try {
    vector<String> strings;
    strings.push_back("/foo");
    strings.push_back("/foo/bar");
    strings.push_back("/foo/bar");
    strings.push_back("hello");
    strings.push_back("help");

    DynamicBuffer buf;
    size_t total_uncompressed_len, total_compressed_len;
    total_uncompressed_len = total_compressed_len = 0;
    size_t compressed_len;
    size_t ii;

    StringCompressorPrefix compressor;
    DynamicBuffer dbuf;
    for (ii=0; ii<strings.size(); ++ii) {
      total_uncompressed_len += strings[ii].size()+1;
      compressor.add(strings[ii].c_str());
      compressed_len = compressor.length();
      total_compressed_len += compressed_len;
      dbuf.ensure(compressed_len);
      compressor.write(dbuf.ptr);
      dbuf.ptr += compressed_len;
    }

    cout << "Compressed strings from " << total_uncompressed_len << "B to "
         << total_compressed_len << "B" << endl;

    const uint8_t *base = dbuf.base;
    StringDecompressorPrefix decompressor;
    String str;
    ii = 0;
    while(base < dbuf.base + dbuf.fill()) {
      base = decompressor.add(base);
      total_compressed_len -= decompressor.length();
      decompressor.load(str);
      if (str != strings[ii]) {
        cout << "Expected " << strings[ii] << " got " << str << endl;
        exit(1);
      }
      ++ii;
    }

    if (ii != strings.size()) {
      cout << "Expected " << strings.size() << " results, got " << ii << endl;
      exit(1);
    }
    if (total_compressed_len != 0) {
      cout << "Expected 0B left after decompression got " <<  total_compressed_len  << endl;
      exit(1);
    }
  }
  catch (Exception &e) {
    cout << e << endl;
    return 1;
  }
}
