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

#include <boost/progress.hpp>
#include <boost/timer.hpp>
#include <boost/thread/xtime.hpp>

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Stopwatch.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/KeySpec.h"

using namespace Hypertable;
using namespace std;

namespace {

  const char cb64[]="ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

  void fill_buffer_with_random_ascii(char *buf, size_t len) {
    size_t in_i=0, out_i=0;
    uint32_t u32;
    uint8_t *in;

    while (out_i < len) {

      u32 = (uint32_t)random();
      in = (uint8_t *)&u32;
      in_i = 0;

      buf[out_i++] = cb64[ in[in_i] >> 2 ];
      if (out_i == len)
	break;

      buf[out_i++] = cb64[ ((in[in_i] & 0x03) << 4) | ((in[in_i+1] & 0xf0) >> 4) ];
      if (out_i == len)
	break;

      buf[out_i++] = cb64[ ((in[in_i+1] & 0x0f) << 2) | ((in[in_i+2] & 0xc0) >> 6) ];
      if (out_i == len)
	break;

      buf[out_i++] = cb64[ in[in_i+2] & 0x3f ];
    
      in_i += 3;
    }

  }

  const char *usage[] = {
    "usage: random_write_test [options] <total-bytes>",
    "",
    "  options:",
    "    --blocksize=<n>  Size of value to write",
    "    --seed=<n>       Random number generator seed",
    "",
    "  This program ...",
    "",
    (const char *)0
  };



}

int main(int argc, char **argv) {
  ClientPtr hypertable_client_ptr;
  TablePtr table_ptr;
  TableMutatorPtr mutator_ptr;
  KeySpec key;
  char random_chars[4194304];
  char *end_random;
  char *value_ptr;
  uint64_t total = 0;
  size_t blocksize = 0;
  unsigned long seed = 1234;

  for (size_t i=1; i<(size_t)argc; i++) {
    if (argv[i][0] == '-') {
      if (!strncmp(argv[i], "--blocksize=", 12)) {
	blocksize = atoi(&argv[i][12]);
      }
      else if (!strncmp(argv[i], "--seed=", 7)) {
	seed = atoi(&argv[i][7]);
      }
    }
    else {
      if (total != 0)
	Usage::dump_and_exit(usage);
      total = strtoll(argv[i], 0, 0);
    }
  }

  if (total == 0)
    Usage::dump_and_exit(usage);

  srandom(seed);

  if (blocksize == 0)
    blocksize = 1000;

  size_t R = total / blocksize;

  fill_buffer_with_random_ascii(random_chars, 4194304);

  end_random = random_chars + (4194304 - blocksize);

  try {
	
    hypertable_client_ptr = new Hypertable::Client(argv[0]);

    table_ptr = hypertable_client_ptr->open_table("PerfEval");

    mutator_ptr = table_ptr->create_mutator();
	
  }
  catch (Hypertable::Exception &e) {
    cerr << "error: " << Error::get_text(e.code()) << " - " << e.what() << endl;
    return 1;
  }

  memset(&key, 0, sizeof(key));
  key.column_family = "Field";

  char key_data[32];

  key.row_len = 12;
  key.row = key_data;  // Row key: a random 12-digit number.
  key_data[key.row_len] = '\0';

  Stopwatch stopwatch;

  {
    boost::progress_display progress_meter(R);

    try {

      value_ptr = random_chars;

      for (size_t i = 0; i < R; ++i) {

	key.row = random_chars + (random() % 4194250);
	memcpy(key_data, key.row, 12);
	key.row = key_data;
      
	mutator_ptr->set(key, value_ptr, blocksize);

	value_ptr += blocksize;
	if (value_ptr > end_random)
	  value_ptr = random_chars;

	progress_meter += 1;
      }

      mutator_ptr->flush();
    }
    catch (Hypertable::Exception &e) {
      cerr << "error: " << Error::get_text(e.code()) << " - " << e.what() << endl;
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

}

