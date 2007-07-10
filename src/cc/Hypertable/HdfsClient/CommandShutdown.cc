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


#include "AsyncComm/CallbackHandlerSynchronizer.h"

#include "Common/Error.h"
#include "Common/Usage.h"

#include "Global.h"
#include "CommandShutdown.h"

using namespace hypertable;

namespace {
  const char *usage[] = {
    "usage: hdfsClient shutdown [now]",
    "",
    "This command sends a shutdown request to the HdfsBroker server.  If the 'now'",
    "argument is given, the HdfsBroker will do an unclean shutdown by exiting",
    "immediately.  Otherwise it will wait for all pending requests to complete",
    "before shutting down.",
    (const char *)0
  };

}



int hypertable::CommandShutdown(vector<const char *> &args) {
  uint32_t msgId = 0;
  CallbackHandlerSynchronizer *handler = new CallbackHandlerSynchronizer();
  uint16_t flags = 0;

  if (args.size() > 0) {
    if (!strcmp(args[0], "now"))
      flags |= HdfsProtocol::SHUTDOWN_FLAG_IMMEDIATE;
    else
      Usage::DumpAndExit(usage);
  }

  Global::client->Shutdown(flags, handler, &msgId);

  delete handler;
  return 0;
}


