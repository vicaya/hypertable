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

#include "Common/Error.h"
#include "Common/Usage.h"

#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "Global.h"
#include "CommandCopyFromLocal.h"

using namespace hypertable;

namespace {
  const int BUFFER_SIZE = 32768;
  const char *usage[] = {
    "usage: hdfsClient copyFromLocal <src> <dst>",
    "",
    "This program copies the local file <src> to the file <dst> in the HDFS.",
    (const char *)0
  };

}

int hypertable::CommandCopyFromLocal(vector<const char *> &args) {
  uint32_t msgId = 0;
  int32_t fd = 0;
  DispatchHandlerSynchronizer *handler = new DispatchHandlerSynchronizer();
  int error = Error::OK;
  EventPtr eventPtr;
  FILE *fp = 0;
  HdfsProtocol::ResponseHeaderWriteT *writeHeader = 0;
  size_t nread;
  int srcArg = 0;
  uint8_t *buf;

  if (args.size() < 2)
    Usage::DumpAndExit(usage);

  if ((fp = fopen(args[srcArg], "r")) == 0) {
    perror(args[1]);
    goto abort;
  }

  if ((error = Global::client->Create(args[srcArg+1], true, -1, -1, -1, &fd)) != Error::OK)
    goto abort;

  // send 3 writes
  for (int i=0; i<3; i++) {
    buf = new uint8_t [ BUFFER_SIZE ];
    if ((nread = fread(buf, 1, BUFFER_SIZE, fp)) == 0)
      goto done;
    if ((error = Global::client->Write(fd, buf, nread, handler, &msgId)) != Error::OK)
      goto done;
  }

  while (true) {

    if (!handler->WaitForReply(eventPtr)) {
      LOG_VA_ERROR("%s", eventPtr->toString().c_str());
      goto abort;
    }

    writeHeader = (HdfsProtocol::ResponseHeaderWriteT *)eventPtr->message;

    //LOG_VA_INFO("Wrote %d bytes at offset %lld", writeHeader->amount, writeHeader->offset);

    buf = new uint8_t [ BUFFER_SIZE ];
    if ((nread = fread(buf, 1, BUFFER_SIZE, fp)) == 0)
      break;
    if ((error = Global::client->Write(fd, buf, nread, handler, &msgId)) != Error::OK)
      goto abort;
  }

 done:

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

