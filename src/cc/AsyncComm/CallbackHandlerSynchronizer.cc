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

