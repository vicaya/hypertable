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


#include "Common/Error.h"
#include "Common/Logger.h"

#include "CallbackHandlerSynchronizer.h"
#include "Protocol.h"

using namespace hypertable;


/**
 *
 */
CallbackHandlerSynchronizer::CallbackHandlerSynchronizer() : mReceiveQueue(), mMutex(), mCond() {
  return;
}



/**
 *
 */
void CallbackHandlerSynchronizer::handle(Event &event) {
  boost::mutex::scoped_lock lock(mMutex);
  mReceiveQueue.push( new Event(event) );
  mCond.notify_one();    
}



/**
 * 
 */
bool CallbackHandlerSynchronizer::WaitForReply(Event **eventPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  Event *event;

  while (mReceiveQueue.empty())
    mCond.wait(lock);
  
  event = mReceiveQueue.front();
  mReceiveQueue.pop();

  *eventPtr = event;

  if (event->type == Event::MESSAGE && Protocol::ResponseCode(event) == Error::OK)
    return true;

  return false;
}


/**
 * 
 */
Event *CallbackHandlerSynchronizer::WaitForReply() {
  boost::mutex::scoped_lock lock(mMutex);
  Event *event = 0;

  while (mReceiveQueue.empty())
    mCond.wait(lock);
  
  event = mReceiveQueue.front();
  mReceiveQueue.pop();

  if (event->type == Event::MESSAGE && Protocol::ResponseCode(event) == Error::OK) {
    delete event;
    return 0;
  }

  return event;
}



/**
 * @deprecated
 */
bool CallbackHandlerSynchronizer::WaitForReply(Event **eventPtr, uint32_t id) {
  boost::mutex::scoped_lock lock(mMutex);
  Event *event;

  while (true) {

    while (mReceiveQueue.empty())
      mCond.wait(lock);
  
    event = mReceiveQueue.front();
    mReceiveQueue.pop();

    if (event->type == Event::MESSAGE && event->header->id == id) {
      *eventPtr = event;
      break;
    }
    else if (event->type != Event::MESSAGE)
      return false;
  }

  return true;
}

