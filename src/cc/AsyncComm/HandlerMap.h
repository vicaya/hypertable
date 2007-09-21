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

#ifndef HYPERTABLE_HANDLERMAP_H
#define HYPERTABLE_HANDLERMAP_H

//#define DISABLE_LOG_DEBUG

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "Common/Logger.h"
#include "Common/SockAddrMap.h"

#include "IOHandlerAccept.h"
#include "IOHandlerData.h"
#include "IOHandlerDatagram.h"

namespace hypertable {

  class HandlerMap {

  public:

    void InsertHandler(IOHandler *handler) {
      boost::mutex::scoped_lock lock(mMutex);
      mHandlerMap[handler->GetAddress()] = handler;
    }

    bool ContainsHandler(struct sockaddr_in &addr) {
      boost::mutex::scoped_lock lock(mMutex);
      return LookupHandler(addr);
    }

    bool LookupDataHandler(struct sockaddr_in &addr, IOHandlerDataPtr &ioHandlerDataPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      IOHandler *handler = LookupHandler(addr);
      if (handler) {
	ioHandlerDataPtr = dynamic_cast<IOHandlerData *>(handler);
	if (ioHandlerDataPtr)
	  return true;
      }
      return false;
    }

    bool LookupAcceptHandler(struct sockaddr_in &addr, IOHandlerAcceptPtr &ioHandlerAcceptPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      IOHandler *handler = LookupHandler(addr);
      if (handler) {
	ioHandlerAcceptPtr = dynamic_cast<IOHandlerAccept *>(handler);
	if (ioHandlerAcceptPtr)
	  return true;
      }
      return false;
    }

    void InsertDatagramHandler(IOHandler *handler) {
      boost::mutex::scoped_lock lock(mMutex);
      mDatagramHandlerMap[handler->GetAddress()] = handler;
    }

    bool LookupDatagramHandler(struct sockaddr_in &addr, IOHandlerDatagramPtr &ioHandlerDatagramPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      SockAddrMapT<IOHandlerPtr>::iterator iter = mDatagramHandlerMap.find(addr);
      if (iter == mHandlerMap.end())
	return false;
      ioHandlerDatagramPtr = (IOHandlerDatagram *)(*iter).second.get();
      return true;
    }

    bool RemoveHandler(struct sockaddr_in &addr, IOHandlerPtr &handlerPtr) {
      SockAddrMapT<IOHandlerPtr>::iterator iter;
      if ((iter = mHandlerMap.find(addr)) != mHandlerMap.end()) {
	handlerPtr = (*iter).second;
	mHandlerMap.erase(iter);
      }
      else if ((iter = mDatagramHandlerMap.find(addr)) != mDatagramHandlerMap.end()) {
	handlerPtr = (*iter).second;
	mDatagramHandlerMap.erase(iter);
      }
      else
	return false;
      return true;
    }

    bool DecomissionHandler(struct sockaddr_in &addr, IOHandlerPtr &handlerPtr) {
      boost::mutex::scoped_lock lock(mMutex);

      if (RemoveHandler(addr, handlerPtr)) {
	mDecomissionedHandlers.insert(handlerPtr);
	return true;
      }
      return false;
    }

    bool DecomissionHandler(struct sockaddr_in &addr) {
      IOHandlerPtr handlerPtr;
      return DecomissionHandler(addr, handlerPtr);
    }

    void PurgeHandler(IOHandler *handler) {
      boost::mutex::scoped_lock lock(mMutex);
      IOHandlerPtr handlerPtr = handler;
      mDecomissionedHandlers.erase(handlerPtr);
      if (mDecomissionedHandlers.empty())
	mCond.notify_all();
    }

    void DecomissionAll(set<IOHandler *> &handlers) {
      boost::mutex::scoped_lock lock(mMutex);
      SockAddrMapT<IOHandlerPtr>::iterator iter;
      
      // TCP handlers
      for (iter = mHandlerMap.begin(); iter != mHandlerMap.end(); iter++) {
	mDecomissionedHandlers.insert((*iter).second);
	handlers.insert((*iter).second.get());
      }
      mHandlerMap.clear();

      // UDP handlers
      for (iter = mDatagramHandlerMap.begin(); iter != mDatagramHandlerMap.end(); iter++) {
	mDecomissionedHandlers.insert((*iter).second);
	handlers.insert((*iter).second.get());
      }
      mDatagramHandlerMap.clear();
    }

    void WaitForEmpty() {
      boost::mutex::scoped_lock lock(mMutex);
      if (!mDecomissionedHandlers.empty())
	mCond.wait(lock);
    }

  private:

    IOHandler *LookupHandler(struct sockaddr_in &addr) {
      SockAddrMapT<IOHandlerPtr>::iterator iter = mHandlerMap.find(addr);
      if (iter == mHandlerMap.end())
	return 0;
      return (*iter).second.get();
    }

    boost::mutex                mMutex;
    boost::condition            mCond;
    SockAddrMapT<IOHandlerPtr>  mHandlerMap;
    SockAddrMapT<IOHandlerPtr>  mDatagramHandlerMap;
    set<IOHandlerPtr, ltiohp>   mDecomissionedHandlers;
  };

}


#endif // HYPERTABLE_HANDLERMAP_H
