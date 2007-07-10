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


#include "Common/Error.h"
#include "Common/Usage.h"

#include "Global.h"
#include "CommandLength.h"

using namespace hypertable;

namespace {
  const char *usage[] = {
    "usage: hdfsClient length <file>",
    "",
    "This command sends a length request for the HDFS file <file> to the HdfsBroker and",
    "displays the file length to stdout.",
    (const char *)0
  };

}

int64_t hypertable::CommandLength(vector<const char *> &args) {
  int64_t length = -1;

  if (args.size() < 1)
    Usage::DumpAndExit(usage);

  Global::client->Length(args[0], &length);
  
  return length;
}

