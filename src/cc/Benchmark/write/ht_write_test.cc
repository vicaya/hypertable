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

#include "AsyncComm/Config.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/KeySpec.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;

namespace {

  const char *usage =
    "usage: write_test [options] <total-bytes>\n\n"
    "Description:\n"
    "  This program will generate various types of write worload"
    "  for testing purposes.";

  struct AppPolicy : Config::Policy {
    static void init_options() {
      cmdline_desc(usage).add_options()
        ("max-keys", i64()->default_value(0),
            "Maximum number of unique keys to generate")
        ("key-size", i32()->default_value(12), "Size of each key")
        ("value-size", i32()->default_value(1000), "Size of each value")
        ("seed", i32()->default_value(1234),
            "Pseudo random number generator seed")
        ;
      cmdline_hidden_desc().add_options()("total-bytes", i64(), "");
      cmdline_positional_desc().add("total-bytes", -1);
    }
  };

  typedef Meta::list<AppPolicy, DefaultPolicy, CommPolicy> Policies;
}

int main(int argc, char **argv) {
  ClientPtr hypertable_client;
  TablePtr table;
  TableMutatorPtr mutator;
  KeySpec key;
  boost::shared_array<char> value_data;
  boost::shared_array<char> key_data;
  char *valuep;

  init_with_policies<Policies>(argc, argv);

  uint64_t total = get_i64("total-bytes");
  uint64_t max_keys = get_i64("max-keys");
  uint32_t value_size = get_i32("value-size");
  uint32_t key_size = get_i32("key-size");
  uint32_t seed = get_i32("seed");

  size_t R = total / value_size;

  if (max_keys == 0)
    max_keys = R;

  uint32_t key_offset;
  uint32_t max_key_offset = (max_keys * key_size) - key_size;

  srandom(seed);
  Random::seed(seed);

  key_data.reset( new char [ max_keys * key_size ] );
  Random::fill_buffer_with_random_ascii(key_data.get(), max_keys * key_size);

  value_data.reset( new char [ R + value_size ] );
  Random::fill_buffer_with_random_ascii(value_data.get(), R + value_size);

  try {
    hypertable_client = new Hypertable::Client();

    table = hypertable_client->open_table("RandomTest");

    mutator = table->create_mutator();
  }
  catch (Hypertable::Exception &e) {
    cerr << e << endl;
    _exit(1);
  }

  key.column_family = "Field";

  char *row_key = new char [ key_size + 1 ];
  key.row_len = key_size;
  key.row = row_key;
  row_key[key.row_len] = '\0';

  Stopwatch stopwatch;

  {
    boost::progress_display progress_meter(R);

    try {
      valuep = value_data.get();

      for (size_t i = 0; i < R; ++i) {

        key_offset = random() % (max_key_offset + 1);
        memcpy(row_key, key_data.get()+key_offset, key_size);

        mutator->set(key, valuep, value_size);

        valuep++;

        progress_meter += 1;

      }
      mutator->flush();
    }
    catch (Hypertable::Exception &e) {
      mutator->show_failed(e, cerr);
      _exit(1);
    }
  }

  stopwatch.stop();

  double total_written = (double)total + (double)(R*12);
  printf("  Elapsed time:  %.2f s\n", stopwatch.elapsed());
  printf(" Total inserts:  %llu\n", (Llu)R);
  printf("    Throughput:  %.2f bytes/s\n",
         total_written / stopwatch.elapsed());
  printf("    Throughput:  %.2f inserts/s\n",
         (double)R / stopwatch.elapsed());
  fflush(stdout);
  _exit(0);
}
