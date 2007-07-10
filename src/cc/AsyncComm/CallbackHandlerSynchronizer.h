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


#ifndef CALLBACKHANDLERSYNCHRONIZER_H
#define CALLBACKHANDLERSYNCHRONIZER_H

#include <queue>

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "CallbackHandler.h"
#include "Event.h"

using namespace std;

namespace hypertable {

  class CallbackHandlerSynchronizer : public CallbackHandler {
  
  public:
    CallbackHandlerSynchronizer();
    virtual void handle(Event &event);
    bool WaitForReply(Event **eventPtr);
    Event *WaitForReply();

    /**
     * @deprecated
     */
    bool WaitForReply(Event **eventPtr, uint32_t id);

  private:
    queue<Event *>    mReceiveQueue;
    boost::mutex      mMutex;
    boost::condition  mCond;
  };
}


#endif // CALLBACKHANDLERSYNCHRONIZER_H
