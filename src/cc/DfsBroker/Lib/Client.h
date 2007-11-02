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

#ifndef HYPERTABLE_DFSBROKER_CLIENT_H
#define HYPERTABLE_DFSBROKER_CLIENT_H

#include <ext/hash_map>

#include <boost/thread/mutex.hpp>

#include "Common/Properties.h"
#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/ConnectionManager.h"

#include "Hypertable/Lib/Filesystem.h"

#include "ClientBufferedReaderHandler.h"

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

#include "Protocol.h"

using namespace hypertable;

namespace hypertable {

  namespace DfsBroker {

    class Client : public Filesystem {
    public:

      Client(ConnectionManager *connManager, struct sockaddr_in &addr, time_t timeout);

      Client(ConnectionManager *connManager, PropertiesPtr &propsPtr);

      bool WaitForConnection(long maxWaitSecs) {
	return mConnectionManager->WaitForConnection(mAddr, maxWaitSecs);
      }

      virtual int Open(std::string &name, DispatchHandler *handler);
      virtual int Open(std::string &name, int32_t *fdp);
      virtual int OpenBuffered(std::string &name, uint32_t bufSize, int32_t *fdp);

      virtual int Create(std::string &name, bool overwrite, int32_t bufferSize,
			 int32_t replication, int64_t blockSize, DispatchHandler *handler);
      virtual int Create(std::string &name, bool overwrite, int32_t bufferSize,
			 int32_t replication, int64_t blockSize, int32_t *fdp);

      virtual int Close(int32_t fd, DispatchHandler *handler);
      virtual int Close(int32_t fd);

      virtual int Read(int32_t fd, uint32_t amount, DispatchHandler *handler);
      virtual int Read(int32_t fd, uint32_t amount, uint8_t *dst, uint32_t *nreadp);

      virtual int Append(int32_t fd, const void *buf, uint32_t amount, DispatchHandler *handler);
      virtual int Append(int32_t fd, const void *buf, uint32_t amount);

      virtual int Seek(int32_t fd, uint64_t offset, DispatchHandler *handler);
      virtual int Seek(int32_t fd, uint64_t offset);

      virtual int Remove(std::string &name, DispatchHandler *handler);
      virtual int Remove(std::string &name);

      virtual int Length(std::string &name, DispatchHandler *handler);
      virtual int Length(std::string &name, int64_t *lenp);

      virtual int Pread(int32_t fd, uint64_t offset, uint32_t amount, DispatchHandler *handler);
      virtual int Pread(int32_t fd, uint64_t offset, uint32_t amount, uint8_t *dst, uint32_t *nreadp);

      virtual int Mkdirs(std::string &name, DispatchHandler *handler);
      virtual int Mkdirs(std::string &name);

      virtual int Flush(int32_t fd, DispatchHandler *handler);
      virtual int Flush(int32_t fd);

      virtual int Rmdir(std::string &name, DispatchHandler *handler);
      virtual int Rmdir(std::string &name);

      virtual int Readdir(std::string &name, DispatchHandler *handler);
      virtual int Readdir(std::string &name, std::vector<std::string> &listing);

      int Status();

      int Shutdown(uint16_t flags, DispatchHandler *handler);

      Protocol *GetProtocolObject() { return mProtocol; }

      time_t GetTimeout() { return mTimeout; }
    
    private:

      int SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler);

      typedef __gnu_cxx::hash_map<uint32_t, ClientBufferedReaderHandler *> BufferedReaderMapT;

      boost::mutex          mMutex;
      Comm                 *mComm;
      struct sockaddr_in    mAddr;
      time_t                mTimeout;
      MessageBuilderSimple *mMessageBuilder;
      Protocol             *mProtocol;
      ConnectionManager    *mConnectionManager;
      BufferedReaderMapT    mBufferedReaderMap;
    };

  }

}


#endif // HYPERTABLE_DFSBROKER_CLIENT_H

