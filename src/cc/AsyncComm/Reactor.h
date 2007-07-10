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


#ifndef HYPERTABLE_REACTOR_H
#define HYPERTABLE_REACTOR_H

#include <boost/thread/thread.hpp>

#include "RequestCache.h"

namespace hypertable {

  class Reactor {

    friend class ReactorFactory;

  public:

    static const int READ_READY;
    static const int WRITE_READY;

    Reactor();

    void operator()();

    void AddRequest(uint32_t id, IOHandler *handler, CallbackHandler *cb, time_t expire) {
      mRequestCache.Insert(id, handler, cb, expire);
    }

    CallbackHandler *RemoveRequest(uint32_t id) {
      return mRequestCache.Remove(id);
    }

    void HandleTimeouts();

    void CancelRequests(IOHandler *handler) {
      mRequestCache.PurgeRequests(handler);
    }


#if defined(__linux__)    
    int pollFd;
#elif defined (__APPLE__)
    int kQueue;
#endif

  protected:
    boost::thread     *threadPtr;
    RequestCache      mRequestCache;
  };

}

#endif // HYPERTABLE_REACTOR_H
