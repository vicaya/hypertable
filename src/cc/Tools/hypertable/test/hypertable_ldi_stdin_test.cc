/**
 * Copyright (C) 2009 Sanjit Jhala(Zvents, Inc.)
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
    "./hypertable_ldi_stdin_test",
    "./hypertable.cfg",
    "./hypertable_ldi_stdin_test_load.hql",
    "./hypertable_ldi_stdin_test_select.hql",
    "./hypertable_ldi_stdin_test.golden",
    0
  };
}


int main(int argc, char **argv) {
  std::string cmd_str;
  std::vector<const char*> client_args;

  System::initialize(argv[0]);

  for (int i=0; required_files[i]; i++) {
    if (!FileUtils::exists(required_files[i])) {
      HT_ERRORF("Unable to find '%s'", required_files[i]);
      return 1;
    }
  }

  /**
   *  hypertable_ldi_stdin_test
   */
  cmd_str = "./hypertable --test-mode --config hypertable.cfg "
      "< hypertable_ldi_stdin_test_load.hql > hypertable_ldi_stdin_test.output 2>&1";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "./hypertable --test-mode --config hypertable.cfg "
      "< hypertable_ldi_stdin_test_select.hql >> hypertable_ldi_stdin_test.output 2>&1";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "diff hypertable_ldi_stdin_test.output hypertable_ldi_stdin_test.golden";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  return 0;
}
