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

#ifndef HYPERTABLE_MASTERCLIENT_H
#define HYPERTABLE_MASTERCLIENT_H

#include <boost/thread/mutex.hpp>

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/DispatchHandler.h"

#include "Common/ReferenceCount.h"

#include "Hyperspace/HandleCallback.h"

using namespace hypertable;
using namespace Hyperspace;

namespace Hyperspace {
  class Session;
}

namespace hypertable {

  class ApplicationQueue;
  class Comm;
  class CommBuf;
  class ConnectionManager;

  class MasterClient : public ReferenceCount {
  public:

    MasterClient(ConnectionManager *connManager, Hyperspace::Session *hyperspace, time_t timeout, ApplicationQueue *appQueue);
    ~MasterClient();

    int InitiateConnection(DispatchHandlerPtr dispatchHandlerPtr);

    bool WaitForConnection(long maxWaitSecs);

    int CreateTable(const char *tableName, const char *schemaString, DispatchHandler *handler);
    int CreateTable(const char *tableName, const char *schemaString);

    int GetSchema(const char *tableName, DispatchHandler *handler);
    int GetSchema(const char *tableName, std::string &schema);

    int Status();

    int RegisterServer(std::string &serverIdStr, DispatchHandler *handler);
    int RegisterServer(std::string &serverIdStr);

    /**
    int ReportSplit(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int ReportSplit(const char *name, int32_t *fdp);
    **/

    int ReloadMaster();

    void SetVerboseFlag(bool verbose) { mVerbose = verbose; }

  private:

    int SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler);

    boost::mutex         mMutex;
    bool                 mVerbose;
    ConnectionManager   *mConnManager;
    Comm                *mComm;
    Hyperspace::Session *mHyperspace;
    time_t               mTimeout;
    ApplicationQueue    *mAppQueue;
    bool                 mInitiated;
    uint64_t             mMasterFileHandle;
    HandleCallbackPtr    mMasterFileCallbackPtr;
    struct sockaddr_in   mMasterAddr;
    std::string          mMasterAddrString;
    DispatchHandlerPtr   mDispatcherHandlerPtr;
  };

  typedef boost::intrusive_ptr<MasterClient> MasterClientPtr;
  

}

#endif // HYPERTABLE_MASTERCLIENT_H
