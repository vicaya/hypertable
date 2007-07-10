/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */



extern "C" {
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
}

#include "Common/FileUtils.h"
#include "Common/Logger.h"
#include "Common/TestHarness.h"
#include "Common/Usage.h"

#include "Hypertable/Schema.h"

using namespace hypertable;
using namespace hypertable;

namespace {
  const char *usage[] = {
    "usage: schemaTest [--golden]",
    "",
    "Validates Schema class.  If --golden is supplied, a new golden",
    "output file 'schemaTest.golden' will be generated",
    0
  };
  const char *badSchemas[] = {
    "tests/bad-schema-1.xml",
    "tests/bad-schema-2.xml",
    "tests/bad-schema-3.xml",
    "tests/bad-schema-4.xml",
    "tests/bad-schema-5.xml",
    "tests/bad-schema-6.xml",
    "tests/bad-schema-7.xml",
    "tests/bad-schema-8.xml",
    "tests/bad-schema-9.xml",
    "tests/good-schema-1.xml",
    0
  };
}


int main(int argc, char **argv) {
  off_t len;
  const char *buf;
  bool golden = false;
  TestHarness harness("schemaTest");
  Schema *schema;

  if (argc > 1) {
    if (!strcmp(argv[1], "--golden"))
      golden = true;
    else
      Usage::DumpAndExit(usage);
  }

  for (int i=0; badSchemas[i] != 0; ++i) {
    if ((buf = FileUtils::FileToBuffer(badSchemas[i], &len)) == 0)
      harness.DisplayErrorAndExit();
    schema = Schema::NewInstance(buf, len);
    if (!schema->IsValid()) {
      LOG_VA_ERROR("Schema Parse Error: %s", schema->GetErrorString());      
    }
    delete schema;
    delete [] buf;
  }

  schema = new Schema();

  schema->OpenAccessGroup();
  schema->SetAccessGroupParameter("name", "default");
  schema->OpenColumnFamily();
  schema->SetColumnFamilyParameter("Name", "default");
  schema->SetColumnFamilyParameter("ExpireDays", "30.0");
  schema->SetColumnFamilyParameter("KeepCopies", "3");
  schema->CloseColumnFamily();
  schema->CloseAccessGroup();

  schema->OpenAccessGroup();
  schema->SetAccessGroupParameter("name", "meta");
  schema->OpenColumnFamily();
  schema->SetColumnFamilyParameter("Name", "language");
  schema->CloseColumnFamily();
  schema->OpenColumnFamily();
  schema->SetColumnFamilyParameter("Name", "checksum");
  schema->CloseColumnFamily();
  schema->CloseAccessGroup();

  std::string schemaStr;
  schema->Render(schemaStr);
  FileUtils::Write(harness.GetLogFileDescriptor(), schemaStr.c_str(), strlen(schemaStr.c_str()));

  schemaStr = "";
  schema->AssignIds();
  schema->Render(schemaStr);
  FileUtils::Write(harness.GetLogFileDescriptor(), schemaStr.c_str(), strlen(schemaStr.c_str()));

  delete schema;

  if (!golden)
    harness.ValidateAndExit("tests/schemaTest.golden");

  harness.RegenerateGoldenFile("tests/schemaTest.golden");

  return 0;
}
