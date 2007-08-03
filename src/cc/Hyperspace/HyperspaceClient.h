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
#ifndef HYPERSPACE_CLIENT_H
#define HYPERSPACE_CLIENT_H

#include "AsyncComm/ConnectionManager.h"

#include "Common/DynamicBuffer.h"

#include "HyperspaceProtocol.h"

/**
 * Forward declarations
 */
namespace hypertable {
  class DispatchHandler;
  class Comm;
  class CommBuf;
  class Properties;
}
namespace boost {
  class thread;
}

using namespace hypertable;

namespace hypertable {

  class HyperspaceClient {
  public:

    HyperspaceClient(Comm *comm, struct sockaddr_in &addr, time_t timeout);

    HyperspaceClient(Comm *comm, Properties *props);

    bool WaitForConnection();

    int Mkdirs(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int Mkdirs(const char *name);

    int Create(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int Create(const char *name);

    int AttrSet(const char *fname, const char *aname, const char *avalue, DispatchHandler *handler, uint32_t *msgIdp);
    int AttrSet(const char *fname, const char *aname, const char *avalue);

    int AttrGet(const char *fname, const char *aname, DispatchHandler *handler, uint32_t *msgIdp);
    int AttrGet(const char *fname, const char *aname, DynamicBuffer &out);

    int AttrDel(const char *fname, const char *aname, DispatchHandler *handler, uint32_t *msgIdp);
    int AttrDel(const char *fname, const char *aname);

    int Exists(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int Exists(const char *name);

    int Delete(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int Delete(const char *name);

    HyperspaceProtocol *Protocol() { return mProtocol; }
    
  private:

    int SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler, uint32_t *msgIdp=0);

    Comm                 *mComm;
    struct sockaddr_in    mAddr;
    time_t                mTimeout;
    HyperspaceProtocol   *mProtocol;
    ConnectionManager     mConnManager;
  };
}


#endif // HYPERSPACE_CLIENT_H

