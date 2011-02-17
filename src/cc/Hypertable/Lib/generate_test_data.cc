/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
#include <cstring>
#include <string>
#include <vector>

extern "C" {
#include <sys/time.h>
}

#include "Common/Error.h"
#include "Common/System.h"
#include "Common/TestHarness.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/Schema.h"

#include "TestData.h"


using namespace Hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: generateTestData [OPTIONS] <tablename>",
    "",
    "OPTINS:",
    "  --config=<fname>  Read configuration properties from <fname>",
    "  --limit=<n>       Only output <n> lines of testdata",
    "  --no-deletes      Don't generate delete records so output can",
    "                    be used with the LOAD DATA FILE HQL command",
    "  --seed=<n>        Seed the randon number generator with <n>",
    "  --timestamps      Generate timestamps",
    "  --qualifier-limit=<n>  Limit the pool of qualifiers to <n>",
    "  --row-limit=<n>   Limit the pool of row keys to <n>",
    "",
    "This program generates random test data to stdout.  It",
    "contacts the master server and fetches the schema for",
    "the given table <tablename> and creates random test",
    "data for the table.  It requires the following input",
    "files to be located under ../demo relative to the",
    "location of the generateTestData binary:",
    "",
    "shakespeare.txt.gz",
    "urls.txt.gz",
    "words.gz",
    "",
    "Approximately 1 out of 50 lines will be row deletes and",
    "1 out of 30 lines will be cell deletes.  The rest are normal",
    "inserts.  Each emitted line has one of the following",
    "formats:",
    "",
    "<timestamp> '\\t' <rowkey> '\\t' <qualifiedcolumn> '\\t' <value>",
    "<timestamp> '\\t' <rowkey> '\\t' <qualifiedcolumn> '\\t' DELETE",
    "<timestamp> '\\t' DELETE",
    "",
    "The <timestamp> field will contain the word AUTO unless the",
    "--timestamp argument was supplied, in which case it will contain",
    "the number of microseconds since the epoch that have elapsed at",
    "the time the line was emitted.",
    0
  };
}


int main(int argc, char **argv) {
  TestData tdata;
  Schema *schema;
  std::string schemaspec;
  struct timeval tval;
  int64_t timestamp = 0;
  uint32_t index;
  const char *rowkey;
  const char *qualifier;
  uint8_t family;
  const char *content, *ptr;
  std::string value;
  size_t cfmax;
  vector<std::string> cfnames;
  int modval;
  std::string cfgfile = "";
  Client *client;
  NamespacePtr ns;
  std::string tablename = "";
  unsigned int seed = 1234;
  bool gen_timestamps = false;
  uint32_t cqlimit = 0;
  uint32_t row_limit = 0;
  uint32_t limit = 0;
  uint32_t emitted = 0;
  bool no_deletes = false;

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      cfgfile = &argv[i][9];
    else if (!strncmp(argv[i], "--seed=", 7)) {
      seed = atoi(&argv[i][7]);
    }
    else if (!strncmp(argv[i], "--qualifier-limit=", 18)) {
      cqlimit = atoi(&argv[i][18]);
    }
    else if (!strncmp(argv[i], "--row-limit=", 12)) {
      row_limit = atoi(&argv[i][12]);
    }
    else if (!strncmp(argv[i], "--limit=", 8)) {
      limit = atoi(&argv[i][8]);
    }
    else if (!strcmp(argv[i], "--timestamps")) {
      gen_timestamps = true;
    }
    else if (!strcmp(argv[i], "--no-deletes")) {
      no_deletes = true;
    }
    else if (tablename == "")
      tablename = argv[i];
    else
      Usage::dump_and_exit(usage);
  }

  if (tablename == "")
    Usage::dump_and_exit(usage);

  client = new Client(System::locate_install_dir(argv[0]), cfgfile);
  ns = client->open_namespace("/");

  if (!tdata.load(System::install_dir + "/demo"))
    exit(1);

  try {
    schemaspec = ns->get_schema_str(tablename);
  }
  catch (Hypertable::Exception &e) {
    HT_ERRORF("Problem getting schema for table '%s' - %s", argv[1], e.what());
    return e.code();
  }

  schema = Schema::new_instance(schemaspec.c_str(), schemaspec.length());
  if (!schema->is_valid()) {
    HT_ERRORF("Schema Parse Error: %s", schema->get_error_string());
    exit(1);
  }

  if (schema->need_id_assignment()) {
    HT_ERROR("Schema Parse Error: needs ID assignment");
    exit(1);
  }

  cfmax = schema->get_max_column_family_id();
  cfnames.resize(cfmax+1);

  foreach(Schema::AccessGroup *ag, schema->get_access_groups()) {
    foreach(Schema::ColumnFamily *cf, ag->columns)
      cfnames[cf->id] = cf->name;
  }

  if (cqlimit == 0)
    cqlimit = tdata.urls.size();
  else if (tdata.urls.size() < cqlimit)
    cqlimit = tdata.urls.size();

  if (row_limit == 0)
    row_limit = tdata.words.size();
  else if (tdata.words.size() < row_limit)
    row_limit = tdata.words.size();

  /**
   * Output header line
   */
  if (gen_timestamps)
    cout << "timestamp\trow\tcolumn\tvalue" << endl;
  else
    cout << "row\tcolumn\tvalue" << endl;

  while (true) {

    if (limit && emitted >= limit)
      break;
    emitted++;

    if (gen_timestamps) {
      // timestamp
      gettimeofday(&tval, 0);
      timestamp = ((int64_t)tval.tv_sec * 1000000LL) + tval.tv_usec;
    }

    // row key
    index = System::rand32() % row_limit;
    rowkey = tdata.words[index].get();

    index = System::rand32();
    modval = System::rand32() % 49;
    if (!no_deletes && (index % 49) == (size_t)modval) {
      if (gen_timestamps)
        cout << timestamp << "\t" << rowkey << "\tDELETE" << endl;
      else
        cout << rowkey << "\tDELETE" << endl;
      continue;
    }

    index = System::rand32();
    family = (index % cfmax) + 1;

    index = System::rand32() % cqlimit;
    qualifier = tdata.urls[index].get();

    index = System::rand32();
    modval = System::rand32() % 29;
    if (!no_deletes && (index % 29) == 0) {
      if (gen_timestamps)
        cout << timestamp << "\t" << rowkey << "\t" << cfnames[family] << ":"
             << qualifier << "\tDELETE" << endl;
      else
        cout << rowkey << "\t" << cfnames[family] << ":" << qualifier
             << "\tDELETE" << endl;
      continue;
    }

    index = System::rand32() % tdata.content.size();
    content = tdata.content[index].get();

    if ((ptr = strchr(content, '\n')) != 0)
      value = std::string(content, ptr-content);
    else
      value = content;

    if (gen_timestamps)
      cout << timestamp << "\t" << rowkey << "\t" << cfnames[family] << ":"
           << qualifier << "\t" << value << endl;
    else
      cout << rowkey << "\t" << cfnames[family] << ":" << qualifier << "\t"
           << value << endl;
  }

  return 0;
}
