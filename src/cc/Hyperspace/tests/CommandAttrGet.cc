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

extern "C" {
#include <stdio.h>
#include <string.h>
}

#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Usage.h"

#include "Hyperspace/HyperspaceClient.h"

#include "CommandAttrGet.h"

using namespace hypertable;


namespace {
  const char *usage[] = {
    "",
    "usage: hyperspace attrget <fname> <attrname>",
    "",
    "This program gets the extended attribute <attrname> for file <fname>",
    "",
    (const char *)0
  };

}

int hypertable::CommandAttrGet(HyperspaceClient *client, vector<const char *> &args) {
  DynamicBuffer valueBuf(0);

  if (args.size() != 2)
    Usage::DumpAndExit(usage);

  int error = client->AttrGet(args[0], args[1], valueBuf);

  if (error != Error::OK)
    return error;

  if (valueBuf.fill() == 0)
    cout << "[NULL]" << endl;
  else
    cout << (char *)valueBuf.buf << endl;

  return error;
}

