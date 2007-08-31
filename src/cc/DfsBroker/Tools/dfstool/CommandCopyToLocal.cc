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
#include "Common/Usage.h"

#include "CommandCopyToLocal.h"

using namespace hypertable;

namespace {
  const int BUFFER_SIZE = 32768;
}


const char *CommandCopyToLocal::msUsage[] = {
  "copyToLocal [--seek=<n>] <src> <dst>",
  "",
  "  This program copies the file <src> from the DFS to the file",
  "  <dst> in the local filesystem.  If --seek is supplied, then",
  "  the source file is seeked to offset <n> before starting the copy.",
  (const char *)0
};


int CommandCopyToLocal::run() {
  DispatchHandlerSynchronizer syncHandler;
  int32_t fd = 0;
  int error = Error::OK;
  EventPtr eventPtr;
  FILE *fp = 0;
  DfsBroker::Protocol::ResponseHeaderReadT *readHeader = 0;
  uint64_t startOffset = 0;
  int srcArg = 0;

  if (mArgs.size() < 2) {
    cerr << "Insufficient number of arguments" << endl;
    return -1;
  }

  if (!strcmp(mArgs[0].first.c_str(), "--seek") && mArgs[0].second != "") {
    startOffset = strtol(mArgs[0].second.c_str(), 0, 10);
    srcArg = 1;
  }

  if ((fp = fopen(mArgs[srcArg+1].first.c_str(), "w+")) == 0) {
    perror(mArgs[srcArg+1].first.c_str());
    goto abort;
  }

  if ((error = mClient->Open(mArgs[srcArg].first, &fd)) != Error::OK)
    goto abort;

  if (startOffset > 0) {
    if ((error = mClient->Seek(fd, startOffset)) != Error::OK)
      goto abort;
  }

  if ((error = mClient->Read(fd, BUFFER_SIZE, &syncHandler)) != Error::OK)
    goto abort;

  if ((error = mClient->Read(fd, BUFFER_SIZE, &syncHandler)) != Error::OK)
    goto abort;

  if ((error = mClient->Read(fd, BUFFER_SIZE, &syncHandler)) != Error::OK)
    goto abort;

  while (syncHandler.WaitForReply(eventPtr)) {

    if ((error = Protocol::ResponseCode(eventPtr)) != Error::OK)
      goto abort;

    readHeader = (DfsBroker::Protocol::ResponseHeaderReadT *)eventPtr->message;

    if (readHeader->amount > 0) {
      if (fwrite(&readHeader[1], readHeader->amount, 1, fp) != 1) {
	perror("write failed");
	goto abort;
      }
    }

    if (readHeader->amount < BUFFER_SIZE) {
      syncHandler.WaitForReply(eventPtr);
      break;
    }

    if ((error = mClient->Read(fd, BUFFER_SIZE, &syncHandler)) != Error::OK)
      goto abort;
  }

  if ((error = mClient->Close(fd)) != Error::OK)
    goto abort;

  abort:
  if (fp)
    fclose(fp);
  if (error != 0)
    cerr << Error::GetText(error) << endl;
  return error;
}

