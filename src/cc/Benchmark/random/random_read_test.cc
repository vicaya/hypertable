/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */
#include "Common/Compat.h"

#include <cstdlib>
#include <cstdio>
#include <climits>
#include <iostream>
#include <fstream>

#include <boost/progress.hpp>
#include <boost/timer.hpp>
#include <boost/thread/xtime.hpp>

#include "Common/Checksum.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Random.h"
#include "Common/Stopwatch.h"
#include "Common/String.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/KeySpec.h"

using namespace Hypertable;
using namespace std;

namespace {

  const char *usage[] = {
    "usage: random_read_test [options] <total-bytes>",
    "",
    "  options:",
    "    --blocksize=<n>         Size of values supplied in write test",
    "    --checksum-file=<file>  Write keys + value checksums to <file>",
    "    --config=<file>         Use <file> as Hypertable config file",
    "    --seed=<n>              Random number generator seed",
    "",
    "  This program ...",
    "",
    (const char *)0
  };



}

int main(int argc, char **argv) {
  ClientPtr hypertable_client_ptr;
  TablePtr table_ptr;
  ScanSpecBuilder scan_spec;
  TableScannerPtr scanner_ptr;
  Cell cell;
  size_t blocksize = 0;
  uint64_t total = 0;
  unsigned long seed = 1234;
  String config_file;
  bool write_checksums = false;
  uint32_t checksum;
  ofstream checksum_out;

  for (size_t i=1; i<(size_t)argc; i++) {
    if (argv[i][0] == '-') {
      if (!strncmp(argv[i], "--blocksize=", 12)) {
        blocksize = atoi(&argv[i][12]);
      }
      else if (!strncmp(argv[i], "--seed=", 7)) {
        seed = atoi(&argv[i][7]);
      }
      else if (!strncmp(argv[i], "--checksum-file=", 16)) {
        checksum_out.open(&argv[i][16]);
        write_checksums = true;
      }
      else if (!strncmp(argv[i], "--config=", 9)) {
        config_file = &argv[i][9];
      }
      else
        Usage::dump_and_exit(usage);
    }
    else {
      if (total != 0)
        Usage::dump_and_exit(usage);
      total = strtoll(argv[i], 0, 0);
    }
  }

  if (total == 0)
    Usage::dump_and_exit(usage);

  System::initialize();

  Random::seed(seed);

  if (blocksize == 0)
    blocksize = 1000;

  size_t R = total / blocksize;

  try {
    if (config_file != "")
      hypertable_client_ptr = new Hypertable::Client(
          System::locate_install_dir(argv[0]), config_file);
    else
      hypertable_client_ptr = new Hypertable::Client(
          System::locate_install_dir(argv[0]));

    table_ptr = hypertable_client_ptr->open_table("RandomTest");

  }
  catch (Hypertable::Exception &e) {
    cerr << "error: " << Error::get_text(e.code()) << " - " << e.what() << endl;
    return 1;
  }

  char key_data[32];

  key_data[12] = '\0';  // Row key: a random 12-digit number.

  Stopwatch stopwatch;

  {
    boost::progress_display progress_meter(R);

    try {

      for (size_t i = 0; i < R; ++i) {

        Random::fill_buffer_with_random_ascii(key_data, 12);

        scan_spec.clear();
        scan_spec.add_column("Field");
        scan_spec.add_row(key_data);

        TableScanner *scanner_ptr=table_ptr->create_scanner(scan_spec.get());
        int n = 0;
        while (scanner_ptr->next(cell)) {
          if (write_checksums) {
            checksum = fletcher32(cell.value, cell.value_len);
            checksum_out << key_data << "\t" << checksum << "\n";
          }
          ++n;
        }
        if (n != 1) {
          printf("Wrong number of results: %d (key=%s, i=%d)\n",
                 n, key_data, (int)i);
          HT_ERROR_OUT << "Wrong number of results: " << n 
              << " (key=" << key_data << ", i=" << i << ")" << HT_END;
          exit(1);
        }
        delete scanner_ptr;

        progress_meter += 1;
      }

    }
    catch (Hypertable::Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      exit(1);
    }
  }

  stopwatch.stop();

  if (write_checksums)
    checksum_out.close();

  double total_read = (double)total + (double)(R*12);

  printf("  Elapsed time:  %.2f s\n", stopwatch.elapsed());
  printf(" Total scanned:  %llu\n", (Llu)R);
  printf("    Throughput:  %.2f bytes/s\n",
         total_read / stopwatch.elapsed());
  printf("    Throughput:  %.2f scanned cells/s\n",
         (double)R / stopwatch.elapsed());
  exit(0); // don't bother with static objects.
}
