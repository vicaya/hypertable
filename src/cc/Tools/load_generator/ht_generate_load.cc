/**
 * Copyright (C) 2009 Sanjit Jhala (Zvents, Inc.)
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

#include <iostream>
#include <cstdio>
#include <cmath>

#include <boost/progress.hpp>
#include <boost/shared_array.hpp>
#include <boost/timer.hpp>
#include <boost/thread/xtime.hpp>

#include "Common/Stopwatch.h"
#include "Common/String.h"
#include "Common/System.h"
#include "Common/Usage.h"
#include "Common/Config.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/DataGenerator.h"
#include "Hypertable/Lib/Config.h"
#include "Hypertable/Lib/Cells.h"

namespace {

  using namespace Hypertable;
  using namespace Hypertable::Config;
  using namespace std;

  const char *usage =
    "Usage: ht_generate_load [options] <config>\n\n"
    "Description:\n"
    "  This program will generate update or query loads.\n"
    "  that can be consumed by LOAD DATA INFILE\n\n"
    "Options";

  struct AppPolicy : Config::Policy {
    static void init_options() {
      cmdline_desc(usage).add_options()
        ("help,h", "Show this help message and exit")
        ("help-config", "Show help message for config properties")
        ("type", str()->default_value("update"),
         "Load type (query/update). Currently only update is supported")
        ("table", str(), "Name of table to query/update")
        ("max-bytes", i64(), "Amount of data to generate, measured by number "
         "of key and value bytes produced")
        ("seed", i32()->default_value(1), "Pseudo-random number generator seed")
        ("verbose,v", boo()->zero_tokens()->default_value(false),
         "Show more verbose output")
        ("flush", boo()->zero_tokens()->default_value(false), "Flush after each update")
        ("version", "Show version information and exit")
        ;
      alias("max-bytes", "DataGenerator.MaxBytes");
      alias("seed", "DataGenerator.Seed");
      cmdline_hidden_desc().add_options()("config", str(), "Configuration file.");
      cmdline_positional_desc().add("config", -1);
    }
  };
}


typedef Meta::list<AppPolicy, DataGeneratorPolicy, DefaultCommPolicy> Policies;

void generate_update_load(String &tablename, bool flush);
double std_dev(uint64_t nn, double sum, double sq_sum);

int main(int argc, char **argv) {

  String load_type, tablename;
  bool flush;

  try {
    init_with_policies<Policies>(argc, argv);

    load_type = get_str("type");
    tablename = get_str("table");
    flush = get_bool("flush");

    if (load_type == "update")
      generate_update_load(tablename, flush);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    exit(1);
  }

  fflush(stdout);
  exit(0); // don't bother with static objects
}

void generate_update_load(String &tablename, bool flush)
{
  double cum_latency=0, cum_sq_latency=0, latency=0;
  double min_latency=10000000, max_latency=0;
  uint64_t total_cells=0;
  uint64_t total_bytes=0;
  Cells cells;
  Stopwatch stopwatch(false);

  try {
    DataGenerator *dg = new DataGenerator();
    ClientPtr hypertable_client_ptr;
    TablePtr table_ptr;
    TableMutatorPtr mutator_ptr;
    String config_file = get_str("config");

    if (config_file != "")
      hypertable_client_ptr = new Hypertable::Client(config_file);
    else
      hypertable_client_ptr = new Hypertable::Client();

    table_ptr = hypertable_client_ptr->open_table(tablename);
    mutator_ptr = table_ptr->create_mutator();

    for (DataGenerator::iterator iter = dg->begin(); iter != dg->end(); iter++) {

      // do update
      cells.clear();
      cells.push_back(*iter);
      stopwatch.reset();
      stopwatch.start();
      mutator_ptr->set_cells(cells);
      if (flush)
        mutator_ptr->flush();
      stopwatch.stop();

      // calculate stats
      ++total_cells;
      latency = stopwatch.elapsed();
      cum_latency += latency;
      cum_sq_latency += pow(latency,2);
      total_bytes += (*iter).value_len + strlen((*iter).row_key)
                     + strlen((*iter).column_qualifier);
      if (latency < min_latency)
        min_latency = latency;
      if (latency > max_latency)
        max_latency = latency;
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    exit(1);
  }

  // spit out stats
  printf("  Total time: %.2f s\n", cum_latency);
  printf("  Total cells inserted: %llu\n", (Llu) total_cells);
  printf("    Throughput (cells/s): %.2f\n", (double)total_cells/cum_latency);
  printf("    Latency (min/max/avg/stddev ms): %.2f/%.2f/%.2f/%.2f ms\n",
      min_latency*1000, max_latency*1000, ((double)cum_latency/total_cells)*1000,
      std_dev(total_cells, cum_latency, cum_sq_latency)*1000);
  printf("  Total bytes inserted: %llu\n", (Llu) total_bytes);
  printf("    Throughput (bytes/s): %.2f\n", (double)total_bytes/cum_latency);

}

/**
 * @param nn Size of set of numbers
 * @param sum Sum of numbers in set
 * @param sq_sum Sum of squares of numbers in set
 * @return std deviation of set
 */
double std_dev(uint64_t nn, double sum, double sq_sum)
{
  double mean = sum/nn;
  double sq_std = sqrt((sq_sum/(double)nn) - pow(mean,2));
  return sq_std;
}
