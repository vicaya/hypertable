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

#ifndef HYPERTABLE_DFSBROKERCLIENT_H
#define HYPERTABLE_DFSBROKERCLIENT_H

#include "Common/Properties.h"
#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/ConnectionManager.h"

#include "Hypertable/Lib/Filesystem.h"

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

#include "DfsBrokerProtocol.h"

using namespace hypertable;

namespace hypertable {

  class DfsBrokerClient : public Filesystem {
  public:

    DfsBrokerClient(ConnectionManager *connManager, struct sockaddr_in &addr, time_t timeout);

    DfsBrokerClient(ConnectionManager *connManager, PropertiesPtr &propsPtr);

    bool WaitForConnection(long maxWaitSecs) {
      return mConnectionManager->WaitForConnection(mAddr, maxWaitSecs);
    }

    virtual int Open(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    virtual int Open(const char *name, int32_t *fdp);

    virtual int Create(const char *name, bool overwrite, int32_t bufferSize,
		       int32_t replication, int64_t blockSize, DispatchHandler *handler, uint32_t *msgIdp);
    virtual int Create(const char *name, bool overwrite, int32_t bufferSize,
		       int32_t replication, int64_t blockSize, int32_t *fdp);

    virtual int Close(int32_t fd, DispatchHandler *handler, uint32_t *msgIdp);
    virtual int Close(int32_t fd);

    virtual int Read(int32_t fd, uint32_t amount, DispatchHandler *handler, uint32_t *msgIdp);
    virtual int Read(int32_t fd, uint32_t amount, uint8_t *dst, uint32_t *nreadp);

    virtual int Append(int32_t fd, uint8_t *buf, uint32_t amount, DispatchHandler *handler, uint32_t *msgIdp);
    virtual int Append(int32_t fd, uint8_t *buf, uint32_t amount);

    virtual int Seek(int32_t fd, uint64_t offset, DispatchHandler *handler, uint32_t *msgIdp);
    virtual int Seek(int32_t fd, uint64_t offset);

    virtual int Remove(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    virtual int Remove(const char *name);

    virtual int Length(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    virtual int Length(const char *name, int64_t *lenp);

    virtual int Pread(int32_t fd, uint64_t offset, uint32_t amount, DispatchHandler *handler, uint32_t *msgIdp);
    virtual int Pread(int32_t fd, uint64_t offset, uint32_t amount, uint8_t *dst, uint32_t *nreadp);

    virtual int Mkdirs(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    virtual int Mkdirs(const char *name);

    int Status();

    int Shutdown(uint16_t flags, DispatchHandler *handler, uint32_t *msgIdp);

    DfsBrokerProtocol *GetProtocolObject() { return mProtocol; }
    
  private:

    int SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler, uint32_t *msgIdp=0);

    Comm                 *mComm;
    struct sockaddr_in    mAddr;
    time_t                mTimeout;
    MessageBuilderSimple *mMessageBuilder;
    DfsBrokerProtocol    *mProtocol;
    ConnectionManager    *mConnectionManager;
  };

}


#endif // HYPERTABLE_DFSBROKERCLIENT_H

