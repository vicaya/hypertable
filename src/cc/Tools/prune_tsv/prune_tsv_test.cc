/**
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <string>

extern "C" {
#include <time.h>
#include <sys/time.h>
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

/**
 *
 */
int main(int argc, char **argv) {
  String cmd_str;
  FILE *fp = fopen("./prune_test.tsv", "w");
  time_t now, past;
  struct tm tms;

  if (fp == 0) {
    cout << "fopen(\"./prune_test.tsv\", \"w\") error - " << strerror(errno) << endl;
    return 1;
  }

  now = time(0);

  past = now - (8 * 86400) + 3600;

  fprintf(fp, "#row\ttimestamp\tc\n");

  for (size_t i=0; i<8; i++) {
    localtime_r(&past, &tms);
    fprintf(fp, "apple\t%d-%02d-%02d %02d:%02d:%02d\t%d\n", tms.tm_year+1900,
            tms.tm_mon+1, tms.tm_mday, tms.tm_hour, tms.tm_min, tms.tm_sec, (int)i);
    past += 86400;
  }

  fclose(fp);

  String hql = (String) " USE 'test';" +
                " DROP TABLE IF EXISTS prune_tsv_test;" +
                " CREATE TABLE prune_tsv_test ( c );" +
                " LOAD DATA INFILE TIMESTAMP_COLUMN=timestamp ROW_KEY_COLUMN=row+timestamp \"./prune_test.tsv\" INTO TABLE prune_tsv_test;" +
                " DUMP TABLE prune_tsv_test INTO FILE \"./prune_tsv_test.output\";"
                ;

  // load from dfs zipped file
  cmd_str = "../hypertable/hypertable --test-mode --config hypertable.cfg --exec '"+ hql + "'";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "./prune_tsv --newer --field 1 4d < ./prune_tsv_test.output > ./regenerated.output";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "./prune_tsv --field 1 4d < ./prune_tsv_test.output | grep -v \"^#\" >> ./regenerated.output";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "diff ./prune_tsv_test.output ./regenerated.output";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "./prune_tsv --newer 4d < ./prune_tsv_test.output > ./regenerated.output";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "./prune_tsv 4d < ./prune_tsv_test.output | grep -v \"^#\" >> ./regenerated.output";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  cmd_str = "diff ./prune_tsv_test.output ./regenerated.output";
  if (system(cmd_str.c_str()) != 0)
    return 1;

  return 0;

}

