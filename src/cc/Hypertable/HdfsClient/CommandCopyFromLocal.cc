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

#include "Common/Error.h"
#include "Common/Usage.h"

#include "AsyncComm/CallbackHandlerSynchronizer.h"

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
  CallbackHandlerSynchronizer *handler = new CallbackHandlerSynchronizer();
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

