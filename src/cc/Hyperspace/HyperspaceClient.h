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
#ifndef HYPERSPACE_CLIENT_H
#define HYPERSPACE_CLIENT_H

#include "AsyncComm/ConnectionManager.h"

#include "Common/DynamicBuffer.h"

#include "HyperspaceProtocol.h"

/**
 * Forward declarations
 */
namespace hypertable {
  class CallbackHandler;
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

    int Mkdirs(const char *name, CallbackHandler *handler, uint32_t *msgIdp);
    int Mkdirs(const char *name);

    int Create(const char *name, CallbackHandler *handler, uint32_t *msgIdp);
    int Create(const char *name);

    int AttrSet(const char *fname, const char *aname, const char *avalue, CallbackHandler *handler, uint32_t *msgIdp);
    int AttrSet(const char *fname, const char *aname, const char *avalue);

    int AttrGet(const char *fname, const char *aname, CallbackHandler *handler, uint32_t *msgIdp);
    int AttrGet(const char *fname, const char *aname, DynamicBuffer &out);

    int AttrDel(const char *fname, const char *aname, CallbackHandler *handler, uint32_t *msgIdp);
    int AttrDel(const char *fname, const char *aname);

    int Exists(const char *name, CallbackHandler *handler, uint32_t *msgIdp);
    int Exists(const char *name);

    HyperspaceProtocol *Protocol() { return mProtocol; }
    
  private:

    int SendMessage(CommBuf *cbuf, CallbackHandler *handler, uint32_t *msgIdp=0);

    Comm                 *mComm;
    struct sockaddr_in    mAddr;
    time_t                mTimeout;
    HyperspaceProtocol   *mProtocol;
    ConnectionManager     mConnManager;
  };
}


#endif // HYPERSPACE_CLIENT_H

