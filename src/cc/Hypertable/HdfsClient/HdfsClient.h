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


#ifndef HDFS_CLIENT_H
#define HDFS_CLIENT_H

#include "AsyncComm/CallbackHandlerSynchronizer.h"
#include "AsyncComm/ConnectionManager.h"


/**
 * Forward declarations
 */
namespace hypertable {
  class CallbackHandler;
  class Comm;
  class CommBuf;
  class MessageBuilderSimple;
}
namespace boost {
  class thread;
}

#include "HdfsProtocol.h"

using namespace hypertable;

namespace hypertable {

  class HdfsClient {
  public:

    HdfsClient(Comm *comm, struct sockaddr_in &addr, time_t timeout);

    bool WaitForConnection(long maxWaitSecs) {
      return mConnectionManager.WaitForConnection(maxWaitSecs);
    }

    int Open(const char *name, CallbackHandler *handler, uint32_t *msgIdp);
    int Open(const char *name, int32_t *fdp);

    int Create(const char *name, bool overwrite, int32_t bufferSize,
	       int32_t replication, int64_t blockSize, CallbackHandler *handler, uint32_t *msgIdp);
    int Create(const char *name, bool overwrite, int32_t bufferSize,
	       int32_t replication, int64_t blockSize, int32_t *fdp);

    int Close(int32_t fd, CallbackHandler *handler, uint32_t *msgIdp);
    int Close(int32_t fd);

    int Read(int32_t fd, uint32_t amount, CallbackHandler *handler, uint32_t *msgIdp);
    int Read(int32_t fd, uint32_t amount, uint8_t *dst, uint32_t *nreadp);

    int Write(int32_t fd, uint8_t *buf, uint32_t amount, CallbackHandler *handler, uint32_t *msgIdp);
    int Write(int32_t fd, uint8_t *buf, uint32_t amount);

    int Seek(int32_t fd, uint64_t offset, CallbackHandler *handler, uint32_t *msgIdp);
    int Seek(int32_t fd, uint64_t offset);

    int Remove(const char *name, CallbackHandler *handler, uint32_t *msgIdp);
    int Remove(const char *name);

    int Shutdown(uint16_t flags, CallbackHandler *handler, uint32_t *msgIdp);

    int Length(const char *name, CallbackHandler *handler, uint32_t *msgIdp);
    int Length(const char *name, int64_t *lenp);

    int Pread(int32_t fd, uint64_t offset, uint32_t amount, CallbackHandler *handler, uint32_t *msgIdp);
    int Pread(int32_t fd, uint64_t offset, uint32_t amount, uint8_t *dst, uint32_t *nreadp);

    int Mkdirs(const char *name, CallbackHandler *handler, uint32_t *msgIdp);
    int Mkdirs(const char *name);

    HdfsProtocol *GetProtocolObject() { return mProtocol; }
    
  private:

    int SendMessage(CommBufPtr &cbufPtr, CallbackHandler *handler, uint32_t *msgIdp=0);

    Comm                 *mComm;
    struct sockaddr_in    mAddr;
    time_t                mTimeout;
    MessageBuilderSimple *mMessageBuilder;
    HdfsProtocol         *mProtocol;
    ConnectionManager     mConnectionManager;
  };

}


#endif // HDFS_CLIENT_H

