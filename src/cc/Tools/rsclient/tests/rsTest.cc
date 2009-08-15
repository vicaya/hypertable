/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
#include <iostream>
#include <string>

extern "C" {
#include <sys/types.h>
#include <unistd.h>
}

#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/System.h"

using namespace Hypertable;
using namespace std;


namespace {
  const char *required_files[] = {
    "../hypertable/hypertable",
    "./ht_rsclient",
    "./hypertable.cfg",
    "./Test1-data.txt",
    "./Test1.cmd",
    "./Test1.golden",
    "./Test2-data.txt",
    "./Test2.cmd",
    "./Test2.golden",
    "./Test3.cmd",
    "./Test4-data.txt",
    "./Test4.cmd",
    "./Test4.golden",
    "./initialize.hql",
    0
  };
}

int main(int argc, char **argv) {
  String cmd_str;

  System::initialize(argv[0]);

  for (int i=0; required_files[i]; i++) {
    if (!FileUtils::exists(required_files[i])) {
      HT_ERRORF("Unable to find '%s'", required_files[i]);
      return 1;
    }
  }

  /**
   * Initialize
   */
  cmd_str = "../hypertable/hypertable --config hypertable.cfg --test-mode"
            " < initialize.hql > init.out";
  if (system(cmd_str.c_str()) != 0)
    return 1;


  /**
   *  Test1
   */
  cmd_str = "./ht_rsclient --test-mode --config hypertable.cfg localhost"
            "< Test1.cmd > Test1.output";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = (String)"diff Test1.output Test1.golden";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  /**
   *  Test2
   */

  cmd_str = "./ht_rsclient --test-mode --config hypertable.cfg localhost"
            " < Test2.cmd > Test2.output";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = (String)"diff Test2.output Test2.golden";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  /**
   *  Test3
   */

  cmd_str = "./ht_rsclient --test-mode --config hypertable.cfg localhost"
            " < Test3.cmd > Test3.output";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = (String)"diff Test3.output Test3.golden";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  /**
   *  Test4
   */

  cmd_str = "./ht_rsclient --test-mode --config hypertable.cfg localhost"
            " < Test4.cmd > Test4.output";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = (std::string)"diff Test4.output Test4.golden";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  return 0;
}
