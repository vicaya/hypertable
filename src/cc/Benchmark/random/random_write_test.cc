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

#include "Common/Init.h"
#include "Common/Checksum.h"
#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Random.h"
#include "Common/Stopwatch.h"
#include "Common/String.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/Config.h"
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
        ("blocksize", i32()->default_value(1000), "Size of value to write")
        ("checksum-file", str(), "File to contain, for each insert, "
            "key '\t' <value-checksum> pairs")
        ("flush", boo()->zero_tokens()->default_value(false),
         "Flush after each write")
        ("no-log-sync", boo()->zero_tokens()->default_value(false),
         "Don't do a commit log sync when buffers are auto flushed")
        ("seed", i32()->default_value(1234), "Random number generator seed")
        ;
      cmdline_hidden_desc().add_options()("total-bytes", i64(), "");
      cmdline_positional_desc().add("total-bytes", -1);
    }
  };
}

typedef Meta::list<AppPolicy, DefaultCommPolicy> Policies;

int main(int argc, char **argv) {
  ClientPtr hypertable_client_ptr;
  NamespacePtr namespace_ptr;
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
  bool flush;
  uint32_t mutator_flags=0;
  bool no_log_sync;

  try {
    init_with_policies<Policies>(argc, argv);

    if (has("checksum-file")) {
      checksum_out.open(get_str("checksum-file").c_str());
      write_checksums = true;
    }
    flush = get_bool("flush");
    no_log_sync = get_bool("no-log-sync");
    blocksize = get_i32("blocksize");
    seed = get_i32("seed");
    total = get_i64("total-bytes");
    Random::seed(seed);
    R = total / blocksize;
    random_chars.reset( new char [ R + blocksize ] );
    Random::fill_buffer_with_random_ascii(random_chars.get(), R + blocksize);
    Random::seed(seed);

    if (no_log_sync)
      mutator_flags |= TableMutator::FLAG_NO_LOG_SYNC;

    try {
      if (config_file != "")
        hypertable_client_ptr = new Hypertable::Client(config_file);
      else
        hypertable_client_ptr = new Hypertable::Client();

      namespace_ptr = hypertable_client_ptr->open_namespace("/");
      table_ptr = namespace_ptr->open_table("RandomTest");
      mutator_ptr = table_ptr->create_mutator(0, mutator_flags);
    }
    catch (Hypertable::Exception &e) {
      cerr << e << endl;
      _exit(1);
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
          if (flush)
            mutator_ptr->flush();

          value_ptr++;

          progress_meter += 1;
        }
        if (!flush)
          mutator_ptr->flush();
      }
      catch (Hypertable::Exception &e) {
        HT_ERROR_OUT << e << HT_END;
        _exit(1);
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
  fflush(stdout);
  _exit(0); // don't bother with static objects
}
