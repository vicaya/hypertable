/**
 * Copyright (C) 2010 Sanjit Jhala(Hypertable, Inc.)
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

#include "Common/StringExt.h"
#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/System.h"
#include "DfsBroker/Lib/Client.h"

using namespace Hypertable;
using namespace std;


namespace {
  const char *required_files[] = {
    "./hypertable_ldi_select_test",
    "./hypertable.cfg",
    "./hypertable_ldi_stdin_test_load.hql",
    "./hypertable_ldi_stdin_test_select.hql",
    "./hypertable_test.tsv.gz",
    "./hypertable_ldi_select_test.golden",
    0
  };
}


int main(int argc, char **argv) {
  String cmd_str;

  if (argc != 2) {
    HT_ERRORF("Usage: %s $INSTALL_DIR", argv[0]);
    return 1;
  }
  String install_dir = argv[1];
  System::initialize(argv[0]);

  for (int i=0; required_files[i]; i++) {
    if (!FileUtils::exists(required_files[i])) {
      HT_ERRORF("Unable to find '%s'", required_files[i]);
      return 1;
    }
  }

  /**
   * LDI and Select using stdin
   */
  cmd_str = "./hypertable --test-mode --config hypertable.cfg "
      "< hypertable_ldi_stdin_test_load.hql > hypertable_ldi_select_test.output 2>&1";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "./hypertable --test-mode --config hypertable.cfg "
      "< hypertable_ldi_stdin_test_select.hql >> hypertable_ldi_select_test.output 2>&1";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  /**
   * LDI and Select using DfsBroker
   */
  String test_dir = install_dir + "/fs/local/hypertable/test/";
  // copy data file to dfs dir

  cmd_str = "rm -rf " + test_dir;
  if (system(cmd_str.c_str()) != 0)
    return 1;
  cmd_str = "mkdir " + test_dir;
  if (system(cmd_str.c_str()) != 0)
    return 1;
  cmd_str = "cp hypertable_test.tsv.gz " + test_dir;
  if (system(cmd_str.c_str()) != 0)
    return 1;

  String hql = (String) "DROP TABLE IF EXISTS hypertable;" +
                " CREATE TABLE hypertable ( TestColumnFamily );" +
                " LOAD DATA INFILE ROW_KEY_COLUMN=rowkey" +
                " \"dfs:///hypertable/test/hypertable_test.tsv.gz\"" +
                " INTO TABLE hypertable;"
                ;
  // load from dfs zipped file
  cmd_str = "./hypertable --test-mode --config hypertable.cfg --exec '"+ hql + "'";
  if (system(cmd_str.c_str()) != 0)
    return 1;
  // select into dfs zipped file
  hql = "SELECT * FROM hypertable INTO FILE \"dfs:///hypertable/test/dfs_select.gz\";";
  cmd_str = "./hypertable --test-mode --config hypertable.cfg --exec '"+ hql + "'";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  // cp from dfs dir
  cmd_str = "cp " + test_dir + "dfs_select.gz .";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "rm -rf " + test_dir;
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "gunzip -f dfs_select.gz";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "cat dfs_select >> hypertable_ldi_select_test.output ";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "diff hypertable_ldi_select_test.output hypertable_ldi_select_test.golden";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  return 0;
}
