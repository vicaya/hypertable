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
#include <boost/shared_array.hpp>
#include <boost/timer.hpp>
#include <boost/thread/xtime.hpp>

#include "Common/Checksum.h"
#include "Common/Config.h"
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
using namespace Hypertable::Config;
using namespace std;


namespace {

  const char *usage =
    "Usage: random_write_test [options] <total-bytes>\n\n"
    "Description:\n"
    "  This program will generate random inserts into a table called\n"
    "  'RandomTest' with a single column called 'Field'.  The keys are\n"
    "  a randomly generated 12 digit number.  The values contain randomly\n"
    "  generated ASCII and are of size 'blocksize'\n\n"
    "Options";

  struct AppPolicy : Config::Policy {
    static void init_options() {
      cmdline_desc(usage).add_options()
        ("blocksize", i32()->default_value(1000), "Size of value to write");
      cmdline_desc(usage).add_options()
        ("checksum-file", str(), "File to contain, for each insert, key '\t' <value-checksum> pairs");
      cmdline_desc(usage).add_options()
        ("seed", i32()->default_value(1234), "Random number generator seed");
      cmdline_hidden_desc().add_options()("total-bytes", i64(), "");
      cmdline_positional_desc().add("total-bytes", -1);
    }
    //static void init() {
    //}

  };
}

typedef Meta::list<AppPolicy, DefaultPolicy> Policies;

int main(int argc, char **argv) {
  ClientPtr hypertable_client_ptr;
  TablePtr table_ptr;
  TableMutatorPtr mutator_ptr;
  KeySpec key;
  boost::shared_array<char> random_chars;
  char *value_ptr;
  uint64_t total = 0;
  size_t blocksize = 0;
  unsigned long seed;
  String config_file;
  bool write_checksums = false;
  uint32_t checksum;
  ofstream checksum_out;
  size_t R;
  Stopwatch stopwatch;

  try {
    init_with_policies<Policies>(argc, argv);

    ReactorFactory::initialize(1);

    blocksize = get_i32("blocksize");

    seed = get_i32("seed");

    if (has("checksum-file")) {
      String checksum_filename = get("checksum-file", String());
      checksum_out.open( checksum_filename.c_str() );
      write_checksums = true;
    }

    total = get_i64("total-bytes");

    System::initialize();

    Random::seed(seed);

    R = total / blocksize;

    random_chars.reset( new char [ R + blocksize ] );

    Random::fill_buffer_with_random_ascii(random_chars.get(), R + blocksize);

    Random::seed(seed);

    try {
      if (config_file != "")
        hypertable_client_ptr = new Hypertable::Client(
             System::locate_install_dir(argv[0]), config_file);
      else
        hypertable_client_ptr = new Hypertable::Client(
             System::locate_install_dir(argv[0]));

      table_ptr = hypertable_client_ptr->open_table("RandomTest");

      mutator_ptr = table_ptr->create_mutator();
    }
    catch (Hypertable::Exception &e) {
      cerr << "error: " << Error::get_text(e.code()) << " - " << e.what() << endl;
      return 1;
    }

    key.column_family = "Field";

    char key_data[32];

    key.row_len = 12;
    key.row = key_data;  // Row key: a random 12-digit number.
    key_data[key.row_len] = '\0';

    {
      boost::progress_display progress_meter(R);

      try {

        value_ptr = random_chars.get();

        for (size_t i = 0; i < R; ++i) {

          Random::fill_buffer_with_random_ascii(key_data, 12);

          if (write_checksums) {
            checksum = fletcher32(value_ptr, blocksize);
            checksum_out << key_data << "\t" << checksum << "\n";
          }
          mutator_ptr->set(key, value_ptr, blocksize);

          value_ptr++;

          progress_meter += 1;

        }

        mutator_ptr->flush();
      }
      catch (Hypertable::Exception &e) {
        HT_ERROR_OUT << e << HT_END;
        exit(1);
      }
    }

    stopwatch.stop();

    if (write_checksums)
      checksum_out.close();
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }

  double total_written = (double)total + (double)(R*12);
  printf("  Elapsed time:  %.2f s\n", stopwatch.elapsed());
  printf(" Total inserts:  %llu\n", (Llu)R);
  printf("    Throughput:  %.2f bytes/s\n",
         total_written / stopwatch.elapsed());
  printf("    Throughput:  %.2f inserts/s\n",
         (double)R / stopwatch.elapsed());
  _exit(0); // don't bother with static objects
}
