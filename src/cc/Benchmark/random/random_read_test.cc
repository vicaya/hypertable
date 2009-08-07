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

#include "Common/Init.h"
#include "Common/Checksum.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Random.h"
#include "Common/Stopwatch.h"
#include "Common/String.h"
#include "Common/Usage.h"

#include "AsyncComm/Config.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/KeySpec.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;


namespace {

  const char *usage =
    "Usage: random_read_test [options] <total-bytes>\n\n"
    "Description:\n"
    "  This program is meant to be used in conjunction with random_write_test.\n"
    "  It will generate and lookup the same sequence of keys as random_write_test\n"
    "  as long as the <total-bytes> value is the same.  It expects to find exactly\n"
    "  one entry for each key.  If it encounters a key that comes up empty or returns\n"
    "  more than one entry, it prints an error message and exit with a return status\n"
    "  of 1, otherwise it displays timing statistics and exits with status 0.\n\n"
    "Options";

  struct AppPolicy : Config::Policy {
    static void init_options() {
      cmdline_desc(usage).add_options()
        ("blocksize", i32()->default_value(1000), "Size of value to write")
        ("checksum-file", str(), "File to contain, for each insert, "
            "key '\t' <value-checksum> pairs")
        ("seed", i32()->default_value(1234), "Random number generator seed")
        ("max-keys", i32()->default_value(0), "Maximum number of keys to lookup")
        ;
      cmdline_hidden_desc().add_options()("total-bytes", i64(), "");
      cmdline_positional_desc().add("total-bytes", -1);
    }
  };
}

typedef Meta::list<AppPolicy, DefaultCommPolicy> Policies;


int main(int argc, char **argv) {
  ClientPtr hypertable_client_ptr;
  TablePtr table_ptr;
  ScanSpecBuilder scan_spec;
  TableScannerPtr scanner_ptr;
  Cell cell;
  size_t blocksize;
  uint64_t total = 0;
  unsigned long seed;
  String config_file;
  bool write_checksums = false;
  uint32_t checksum;
  ofstream checksum_out;
  uint32_t max_keys;
  size_t R;

  try {
    init_with_policies<Policies>(argc, argv);

    if (has("checksum-file")) {
      checksum_out.open(get_str("checksum-file").c_str());
      write_checksums = true;
    }
    blocksize = get_i32("blocksize");
    seed = get_i32("seed");
    total = get_i64("total-bytes");
    max_keys = get_i32("max-keys");

    Random::seed(seed);

    R = total / blocksize;

    if (max_keys != 0 && max_keys < R)
      R = max_keys;

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
    _exit(1);
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
          _exit(1);
        }
        delete scanner_ptr;

        progress_meter += 1;
      }

    }
    catch (Hypertable::Exception &e) {
      HT_ERROR_OUT << e << HT_END;
      _exit(1);
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
  fflush(stdout);
  _exit(0); // don't bother with static objects.
}
