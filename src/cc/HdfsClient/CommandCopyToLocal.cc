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
#include <string.h>
}

#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "Common/Error.h"
#include "Common/Usage.h"

#include "Global.h"
#include "CommandCopyToLocal.h"

using namespace hypertable;

namespace {
  const int BUFFER_SIZE = 32768;
  const char *usage[] = {
    "usage: hdfsClient copyToLocal [--seek=<n>] <src> <dst>",
    "",
    "This program copies the file <src> from the local file system to the",
    "file <dst> in DFS. <dst> must be a file, not a directory.  If --seek",
    "is supplied, then the source file is seeked to <n> before starting the",
    "copy.",
    (const char *)0
  };

}

int hypertable::CommandCopyToLocal(vector<const char *> &args) {
  uint32_t msgId = 0;
  int32_t fd = 0;
  DispatchHandlerSynchronizer *handler = new DispatchHandlerSynchronizer();
  int error = Error::OK;
  EventPtr eventPtr;
  FILE *fp = 0;
  HdfsProtocol::ResponseHeaderReadT *readHeader = 0;
  int srcArg = 0;
  uint64_t startOffset = 0;

  if (args.size() < 2)
    Usage::DumpAndExit(usage);

  if (!strncmp(args[0], "--seek=", 7)) {
    startOffset = strtol(&args[0][7], 0, 10);
    srcArg = 1;
  }

  if ((fp = fopen(args[srcArg+1], "w+")) == 0) {
    perror(args[1]);
    goto abort;
  }

  if ((error = Global::client->Open(args[srcArg], &fd)) != Error::OK)
    goto abort;

  if (startOffset > 0) {
    if ((error = Global::client->Seek(fd, startOffset)) != Error::OK)
      goto abort;
  }

  if ((error = Global::client->Read(fd, BUFFER_SIZE, handler, &msgId)) != Error::OK)
    goto abort;

  if ((error = Global::client->Read(fd, BUFFER_SIZE, handler, &msgId)) != Error::OK)
    goto abort;

  if ((error = Global::client->Read(fd, BUFFER_SIZE, handler, &msgId)) != Error::OK)
    goto abort;

  while (handler->WaitForReply(eventPtr)) {

    if ((error = Global::protocol->ResponseCode(eventPtr)) != Error::OK)
      goto abort;

    readHeader = (HdfsProtocol::ResponseHeaderReadT *)eventPtr->message;

    if (readHeader->amount > 0) {
      if (fwrite(&readHeader[1], readHeader->amount, 1, fp) != 1) {
	perror("write failed");
	goto abort;
      }
    }

    if (readHeader->amount < BUFFER_SIZE) {
      handler->WaitForReply(eventPtr, msgId);
      break;
    }

    if ((error = Global::client->Read(fd, BUFFER_SIZE, handler, &msgId)) != Error::OK)
      goto abort;
  }

  if ((error = Global::client->Close(fd)) != Error::OK)
    goto abort;

  abort:
  if (fp)
    fclose(fp);
  if (error != 0)
    cerr << Error::GetText(error) << endl;
  delete handler;
  return error;
}

