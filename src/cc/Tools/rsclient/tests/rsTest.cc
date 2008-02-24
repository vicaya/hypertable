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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

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
  const char *requiredFiles[] = {
    "../hypertable/hypertable",
    "./rsclient",
    "./hypertable.cfg",
    "./Test1-data.txt",
    "./Test1.cmd",
    "./Test1.golden",
    "./Test2-data.txt",
    "./Test2.cmd",
    "./Test2.golden",
    "./Test3.cmd",
    "./initialize.hql",
    0
  };
}

int main(int argc, char **argv) {
  std::string commandStr;

  System::initialize(argv[0]);

  for (int i=0; requiredFiles[i]; i++) {
    if (!FileUtils::exists(requiredFiles[i])) {
      HT_ERRORF("Unable to find '%s'", requiredFiles[i]);
      return 1;
    }
  }

  /**
   * Initialize
   */
  commandStr = (std::string)"../hypertable/hypertable --config=hypertable.cfg --batch < initialize.hql > init.out";
  if (system(commandStr.c_str()) != 0)
    return 1;


  /**
   *  Test1
   */
  commandStr = (std::string)"./rsclient --config=hypertable.cfg < Test1.cmd > Test1.output";
  if (system(commandStr.c_str()) != 0)
    return 1;

  commandStr = (std::string)"diff Test1.output Test1.golden";
  if (system(commandStr.c_str()) != 0)
    return 1;

  /**
   *  Test2
   */

  commandStr = (std::string)"./rsclient --config=hypertable.cfg < Test2.cmd > Test2.output";
  if (system(commandStr.c_str()) != 0)
    return 1;

  commandStr = (std::string)"diff Test2.output Test2.golden";
  if (system(commandStr.c_str()) != 0)
    return 1;

  /**
   *  Test3
   */

  commandStr = (std::string)"./rsclient --config=hypertable.cfg < Test3.cmd > Test3.output";
  if (system(commandStr.c_str()) != 0)
    return 1;

  commandStr = (std::string)"diff Test3.output Test3.golden";
  if (system(commandStr.c_str()) != 0)
    return 1;

  return 0;
}
