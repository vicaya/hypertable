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


#include "AsyncComm/DispatchHandlerSynchronizer.h"

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
  DispatchHandlerSynchronizer *handler = new DispatchHandlerSynchronizer();
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


