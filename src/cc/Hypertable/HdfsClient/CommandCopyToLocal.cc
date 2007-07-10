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
#include <stdio.h>
#include <string.h>
}

#include "AsyncComm/CallbackHandlerSynchronizer.h"

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
  CallbackHandlerSynchronizer *handler = new CallbackHandlerSynchronizer();
  int error = 1;
  Event *event = 0;
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

  if (Global::client->Open(args[srcArg], &fd) != Error::OK)
    goto abort;

  if (startOffset > 0) {
    if (Global::client->Seek(fd, startOffset) != Error::OK)
      goto abort;
  }

  if (Global::client->Read(fd, BUFFER_SIZE, handler, &msgId) != Error::OK)
    goto abort;

  if (Global::client->Read(fd, BUFFER_SIZE, handler, &msgId) != Error::OK)
    goto abort;

  if (Global::client->Read(fd, BUFFER_SIZE, handler, &msgId) != Error::OK)
    goto abort;

  while (handler->WaitForReply(&event)) {

    if (Global::protocol->ResponseCode(event) != Error::OK)
      goto abort;

    readHeader = (HdfsProtocol::ResponseHeaderReadT *)event->message;

    if (readHeader->amount > 0) {
      if (fwrite(&readHeader[1], readHeader->amount, 1, fp) != 1) {
	perror("write failed");
	goto abort;
      }
    }

    if (readHeader->amount < BUFFER_SIZE) {
      handler->WaitForReply(&event, msgId);
      break;
    }

    if (Global::client->Read(fd, BUFFER_SIZE, handler, &msgId) != Error::OK)
      goto abort;
  }

  if (Global::client->Close(fd) != Error::OK)
    goto abort;

  error = 0;
  
  abort:
  if (fp)
    fclose(fp);
  if (event) {
    if (error != 0)
      cerr << Global::protocol->StringFormatMessage(event) << endl;
    delete event;
  }
  delete handler;
  return error;

}

