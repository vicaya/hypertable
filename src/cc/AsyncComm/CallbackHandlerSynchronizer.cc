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
void CallbackHandlerSynchronizer::handle(EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(mMutex);
  mReceiveQueue.push(eventPtr);
  mCond.notify_one();    
}



/**
 * 
 */
bool CallbackHandlerSynchronizer::WaitForReply(EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(mMutex);

  while (mReceiveQueue.empty())
    mCond.wait(lock);

  eventPtr = mReceiveQueue.front();
  mReceiveQueue.pop();

  if (eventPtr->type == Event::MESSAGE && Protocol::ResponseCode(eventPtr.get()) == Error::OK)
    return true;

  return false;
}


/**
 * @deprecated
 */
bool CallbackHandlerSynchronizer::WaitForReply(EventPtr &eventPtr, uint32_t id) {
  boost::mutex::scoped_lock lock(mMutex);

  while (true) {

    while (mReceiveQueue.empty())
      mCond.wait(lock);
  
    eventPtr = mReceiveQueue.front();
    mReceiveQueue.pop();

    if (eventPtr->type == Event::MESSAGE && eventPtr->header->id == id)
      break;
    else if (eventPtr->type != Event::MESSAGE)
      return false;
  }

  return true;
}

