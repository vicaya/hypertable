/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#ifndef HDFS_CLIENT_H
#define HDFS_CLIENT_H

#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/ConnectionManager.h"


/**
 * Forward declarations
 */
namespace hypertable {
  class DispatchHandler;
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

    int Open(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int Open(const char *name, int32_t *fdp);

    int Create(const char *name, bool overwrite, int32_t bufferSize,
	       int32_t replication, int64_t blockSize, DispatchHandler *handler, uint32_t *msgIdp);
    int Create(const char *name, bool overwrite, int32_t bufferSize,
	       int32_t replication, int64_t blockSize, int32_t *fdp);

    int Close(int32_t fd, DispatchHandler *handler, uint32_t *msgIdp);
    int Close(int32_t fd);

    int Read(int32_t fd, uint32_t amount, DispatchHandler *handler, uint32_t *msgIdp);
    int Read(int32_t fd, uint32_t amount, uint8_t *dst, uint32_t *nreadp);

    int Write(int32_t fd, uint8_t *buf, uint32_t amount, DispatchHandler *handler, uint32_t *msgIdp);
    int Write(int32_t fd, uint8_t *buf, uint32_t amount);

    int Seek(int32_t fd, uint64_t offset, DispatchHandler *handler, uint32_t *msgIdp);
    int Seek(int32_t fd, uint64_t offset);

    int Remove(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int Remove(const char *name);

    int Shutdown(uint16_t flags, DispatchHandler *handler, uint32_t *msgIdp);

    int Length(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int Length(const char *name, int64_t *lenp);

    int Pread(int32_t fd, uint64_t offset, uint32_t amount, DispatchHandler *handler, uint32_t *msgIdp);
    int Pread(int32_t fd, uint64_t offset, uint32_t amount, uint8_t *dst, uint32_t *nreadp);

    int Mkdirs(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int Mkdirs(const char *name);

    HdfsProtocol *GetProtocolObject() { return mProtocol; }
    
  private:

    int SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler, uint32_t *msgIdp=0);

    Comm                 *mComm;
    struct sockaddr_in    mAddr;
    time_t                mTimeout;
    MessageBuilderSimple *mMessageBuilder;
    HdfsProtocol         *mProtocol;
    ConnectionManager     mConnectionManager;
  };

}


#endif // HDFS_CLIENT_H

