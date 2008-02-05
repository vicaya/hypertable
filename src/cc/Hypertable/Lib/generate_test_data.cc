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

#include <cstring>
#include <string>
#include <vector>

extern "C" {
#include <string.h>
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
    "usage: generateTestData [OPTIONS] <tableName>",
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
    "the given table <tableName> and creates random test",
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
    "<timestamp> '\\t' <rowKey> '\\t' <qualifiedColumn> '\\t' <value>",
    "<timestamp> '\\t' <rowKey> '\\t' <qualifiedColumn> '\\t' DELETE",
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
  std::string schemaSpec;
  struct timeval tval;
  uint64_t timestamp = 0;
  uint32_t index;
  const char *rowKey;
  const char *qualifier;
  uint8_t family;
  const char *content, *ptr;
  std::string value;
  size_t cfMax;
  vector<std::string> cfNames;
  int modValue;
  std::string configFile = "";
  Client *client;
  int error;
  std::string tableName = "";
  unsigned int seed = 1234;
  bool generateTimestamps = false;
  uint32_t qualifierLimit = 0;
  uint32_t rowLimit = 0;
  uint32_t limit = 0;
  uint32_t emitted = 0;
  bool no_deletes = false;

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      configFile = &argv[i][9];
    else if (!strncmp(argv[i], "--seed=", 7)) {
      seed = atoi(&argv[i][7]);
    }
    else if (!strncmp(argv[i], "--qualifier-limit=", 18)) {
      qualifierLimit = atoi(&argv[i][18]);
    }
    else if (!strncmp(argv[i], "--row-limit=", 12)) {
      rowLimit = atoi(&argv[i][12]);
    }
    else if (!strncmp(argv[i], "--limit=", 8)) {
      limit = atoi(&argv[i][8]);
    }
    else if (!strcmp(argv[i], "--timestamps")) {
      generateTimestamps = true;
    }
    else if (!strcmp(argv[i], "--no-deletes")) {
      no_deletes = true;
    }
    else if (tableName == "")
      tableName = argv[i];
    else
      Usage::dump_and_exit(usage);
  }

  if (tableName == "")
    Usage::dump_and_exit(usage);

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  if (!tdata.load(System::installDir + "/demo"))
    exit(1);

  client = new Client(argv[0], configFile);

  if ((error = client->get_schema(tableName, schemaSpec)) != Error::OK) {
    HT_ERRORF("Problem getting schema for table '%s' - %s", argv[1], Error::get_text(error));
    return error;
  }

  schema = Schema::new_instance(schemaSpec.c_str(), strlen(schemaSpec.c_str()), true);
  if (!schema->is_valid()) {
    HT_ERRORF("Schema Parse Error: %s", schema->get_error_string());
    exit(1);
  }

  cfMax = schema->get_max_column_family_id();
  cfNames.resize(cfMax+1);

  list<Schema::AccessGroup *> *lgList = schema->get_access_group_list();
  for (list<Schema::AccessGroup *>::iterator iter = lgList->begin(); iter != lgList->end(); iter++) {
    for (list<Schema::ColumnFamily *>::iterator cfIter = (*iter)->columns.begin(); cfIter != (*iter)->columns.end(); cfIter++)
      cfNames[(*cfIter)->id] = (*cfIter)->name;
  }

  if (qualifierLimit == 0)
    qualifierLimit = tdata.urls.size();
  else if (tdata.urls.size() < qualifierLimit)
    qualifierLimit = tdata.urls.size();

  if (rowLimit == 0)
    rowLimit = tdata.words.size();
  else if (tdata.words.size() < rowLimit)
    rowLimit = tdata.words.size();

  srand(seed);

  /**
   * Output header line
   */
  if (generateTimestamps)
    cout << "timestamp\trowkey\tcolumnkey\tvalue" << endl;
  else
    cout << "rowkey\tcolumnkey\tvalue" << endl;

  while (true) {

    if (limit && emitted >= limit)
      break;
    emitted++;

    if (generateTimestamps) {
      // timestamp
      gettimeofday(&tval, 0);
      timestamp = ((uint64_t)tval.tv_sec * 1000000LL) + tval.tv_usec;
    }

    // row key
    index = rand() % rowLimit;
    rowKey = tdata.words[index].get();

    index = rand();
    modValue = rand() % 49;
    if (!no_deletes && (index % 49) == (size_t)modValue) {
      if (generateTimestamps)
	cout << timestamp << "\t" << rowKey << "\tDELETE" << endl;
      else
	cout << rowKey << "\tDELETE" << endl;	
      continue;
    }

    index = rand();
    family = (index % cfMax) + 1;
    
    index = rand() % qualifierLimit;
    qualifier = tdata.urls[index].get();

    index = rand();
    modValue = rand() % 29;
    if (!no_deletes && (index % 29) == 0) {
      if (generateTimestamps)
	cout << timestamp << "\t" << rowKey << "\t" << cfNames[family] << ":" << qualifier << "\tDELETE" << endl;
      else
	cout << rowKey << "\t" << cfNames[family] << ":" << qualifier << "\tDELETE" << endl;
      continue;
    }

    index = rand() % tdata.content.size();
    content = tdata.content[index].get();
    
    if ((ptr = strchr(content, '\n')) != 0)
      value = std::string(content, ptr-content);
    else
      value = content;

    if (generateTimestamps)
      cout << timestamp << "\t" << rowKey << "\t" << cfNames[family] << ":" << qualifier << "\t" << value << endl;    
    else
      cout << rowKey << "\t" << cfNames[family] << ":" << qualifier << "\t" << value << endl;    
  }

  return 0;
}
