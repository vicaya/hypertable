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


#include <iostream>
#include <string>

#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "../CommitLogReaderLocal.h"

using namespace hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: commitLogReaderTest <commitLogDir>",
    "",
    0
  };
}


int main(int argc, char **argv) {
  string logDir;
  CommitLogHeaderT *header;  

  System::Initialize(argv[0]);
  
  if (argc != 2)
    Usage::DumpAndExit(usage);

  logDir = argv[1];
  std::string dummy = "";

  CommitLogReader *clogReader = new CommitLogReaderLocal(logDir, dummy);

  clogReader->InitializeRead(0);

  while (clogReader->NextBlock(&header)) {
    cout << "Block table='" << (char *)&header[1] << "' size=" << header->length << " timestamp=" << header->timestamp << endl;
  }

  return 0;
}
