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

extern "C" {
#include <stdio.h>
}

#include "Common/Error.h"
#include "Common/Logger.h"

#include "CommandCopyFromLocal.h"

using namespace hypertable;

namespace {
  const int BUFFER_SIZE = 32768;
}


const char *CommandCopyFromLocal::msUsage[] = {
  "copyFromLocal <src> <dst>",
  "",
  "  This program copies the local file <src> to the remote",
  "  file <dst> in the DFS.",
  (const char *)0
};


int CommandCopyFromLocal::run() {
  DispatchHandlerSynchronizer syncHandler;
  int32_t fd = 0;
  int error = Error::OK;
  EventPtr eventPtr;
  FILE *fp = 0;
  DfsBroker::Protocol::ResponseHeaderAppendT *appendHeader = 0;
  size_t nread;
  int srcArg = 0;
  uint8_t *buf;

  if (mArgs.size() != 2) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if ((fp = fopen(mArgs[srcArg].first.c_str(), "r")) == 0) {
    perror(mArgs[srcArg].first.c_str());
    goto abort;
  }

  if ((error = mClient->Create(mArgs[srcArg+1].first, true, -1, -1, -1, &fd)) != Error::OK)
    goto abort;

  // send 3 appends
  for (int i=0; i<3; i++) {
    buf = new uint8_t [ BUFFER_SIZE ];
    if ((nread = fread(buf, 1, BUFFER_SIZE, fp)) == 0)
      goto done;
    if ((error = mClient->Append(fd, buf, nread, &syncHandler)) != Error::OK)
      goto done;
  }

  while (true) {

    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("%s", eventPtr->toString().c_str());
      goto abort;
    }

    appendHeader = (DfsBroker::Protocol::ResponseHeaderAppendT *)eventPtr->message;

    //LOG_VA_INFO("Wrote %d bytes at offset %lld", appendHeader->amount, appendHeader->offset);

    buf = new uint8_t [ BUFFER_SIZE ];
    if ((nread = fread(buf, 1, BUFFER_SIZE, fp)) == 0)
      break;
    if ((error = mClient->Append(fd, buf, nread, &syncHandler)) != Error::OK)
      goto abort;
  }

 done:

  if ((error = mClient->Close(fd)) != Error::OK)
    goto abort;
    
  abort:
  if (fp)
    fclose(fp);
  if (error != 0)
    cerr << Error::GetText(error) << endl;
  return error;
}
