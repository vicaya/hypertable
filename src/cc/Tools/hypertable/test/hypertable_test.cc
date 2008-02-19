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
    "./hypertable_test",
    "./hypertable.cfg",
    "./hypertable_test.hql",
    "./hypertable_test.golden",
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
   *  hypertable_test
   */
  commandStr = (std::string)"./hypertable --no-prompt --config=hypertable.cfg < hypertable_test.hql > hypertable_test.output 2>&1";
  if (system(commandStr.c_str()) != 0)
    return 1;

  commandStr = (std::string)"diff hypertable_test.output hypertable_test.golden";
  if (system(commandStr.c_str()) != 0)
    return 1;

  return 0;
}
