/**
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#include "Common/Config.h"

#include "Hypertable/Lib/DataGenerator.h"

using namespace Hypertable;
using namespace Hypertable::Config;
using namespace std;

namespace {

  const char *usage =
    "Usage: ht_generate_data [options] <config-file>\n\n"
    "Description:\n"
    "  This program will generate random data controlled by <config-file>\n"
    "  that can be consumed by LOAD DATA INFILE\n\n"
    "Options";

  struct AppPolicy : Config::Policy {
    static void init_options() {
      cmdline_desc(usage).add_options()
        ("help,h", "Show this help message and exit")
        ("help-config", "Show help message for config properties")
        ("max-bytes", i64(), "Amount of data to generate, measured by number "
         "of key and value bytes produced")
        ("seed", i32()->default_value(1), "Pseudo-random number generator seed")
        ("verbose,v", boo()->zero_tokens()->default_value(false),
         "Show more verbose output")
        ("version", "Show version information and exit")
        ;
      alias("max-bytes", "DataGenerator.MaxBytes");
      alias("seed", "DataGenerator.Seed");

      cmdline_hidden_desc().add_options()("config", str(), "Configuration file.");
      cmdline_positional_desc().add("config", -1);
    }
  };
}


typedef Meta::list<AppPolicy, DataGeneratorPolicy> Policies;

int main(int argc, char **argv) {

  try {
    init_with_policies<Policies>(argc, argv);
    DataGenerator *dg = new DataGenerator();

    cout << "rowkey\tcolumnkey\tvalue\n";

    for (DataGenerator::iterator iter = dg->begin(); iter != dg->end(); iter++) {
      cout << (*iter).row_key << "\t" << (*iter).column_family << ":" 
           << (*iter).column_qualifier << "\t" << (const char *)(*iter).value << endl;
    }

  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    exit(1);
  }

  fflush(stdout);
  exit(0); // don't bother with static objects
}
