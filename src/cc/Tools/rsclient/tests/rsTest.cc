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
#include <fstream>
#include <string>

extern "C" {
#include <sys/types.h>
#include <unistd.h>
}

#include "Common/FileUtils.h"
#include "Common/Logger.h"

using namespace hypertable;
using namespace std;

int main(int argc, char **argv) {
  char tmpfile[64];
  std::string commandStr;

  if (!FileUtils::Exists("./rsclient")) {
    LOG_ERROR("Unable to find ./rsclient");
    return 1;
  }

  if (!FileUtils::Exists("./testdata.txt")) {
    LOG_ERROR("Unable to find ./testdata.txt");
    return 1;
  }

  if (!FileUtils::Exists("./hypertable.cfg")) {
    LOG_ERROR("Unable to find ./hypertable.cfg");
    return 1;
  }

  sprintf(tmpfile, "/tmp/rsTest%d", getpid());
  fstream filestr (tmpfile, fstream::out);
  filestr << "load range Test1[:kerchoo]" << endl;
  filestr << "update Test1[:kerchoo] testdata.txt" << endl;
  filestr << "create scanner Test1[:kerchoo]" << endl;
  filestr << "quit" << endl;
  filestr.close();

  commandStr = (std::string)"./rsclient --config=./hypertable.cfg < " + tmpfile + " > rsTest.output";
  if (system(commandStr.c_str()) != 0) {
    unlink(tmpfile);
    return 1;
  }

  unlink(tmpfile);

  commandStr = (std::string)"diff rsTest.output rsTest.golden";
  if (system(commandStr.c_str()) != 0)
    return 1;

  return 0;
}
