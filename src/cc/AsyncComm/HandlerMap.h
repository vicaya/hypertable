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

#include <boost/thread/mutex.hpp>

#include "Common/Logger.h"
#include "Common/SockAddrMap.h"

#include "IOHandlerAccept.h"
#include "IOHandlerData.h"
#include "IOHandlerDatagram.h"

namespace hypertable {

  class HandlerMap {

  public:

    bool LookupDataHandler(struct sockaddr_in &addr, IOHandlerDataPtr &ioHandlerDataPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      SockAddrMapT<IOHandlerDataPtr>::iterator iter = mDataMap.find(addr);
      if (iter == mDataMap.end())
	return false;
      ioHandlerDataPtr = (*iter).second;
      return true;
    }

    void InsertDataHandler(IOHandlerDataPtr &ioHandlerDataPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      mDataMap[ioHandlerDataPtr->GetAddress()] = ioHandlerDataPtr;
    }

    void RemoveDataHandler(struct sockaddr_in &addr) {
      boost::mutex::scoped_lock lock(mMutex);
      SockAddrMapT<IOHandlerDataPtr>::iterator iter = mDataMap.find(addr);
      assert(iter != mDataMap.end());
      mDataMap.erase(iter);
    }

    bool LookupAcceptHandler(struct sockaddr_in &addr, IOHandlerAcceptPtr &ioHandlerAcceptPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      SockAddrMapT<IOHandlerAcceptPtr>::iterator iter = mAcceptMap.find(addr);
      if (iter == mAcceptMap.end())
	return false;
      ioHandlerAcceptPtr = (*iter).second;
      return true;
    }

    void InsertAcceptHandler(IOHandlerAcceptPtr &ioHandlerAcceptPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      mAcceptMap[ioHandlerAcceptPtr->GetAddress()] = ioHandlerAcceptPtr;
    }

    void RemoveAcceptHandler(struct sockaddr_in &addr) {
      boost::mutex::scoped_lock lock(mMutex);
      SockAddrMapT<IOHandlerAcceptPtr>::iterator iter = mAcceptMap.find(addr);
      assert(iter != mAcceptMap.end());
      mAcceptMap.erase(iter);
    }

    void InsertDatagramHandler(uint16_t port, IOHandlerDatagramPtr &ioHandlerDatagramPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      mDatagramMap[port] = ioHandlerDatagramPtr;
    }

    bool LookupDatagramHandler(uint16_t port, IOHandlerDatagramPtr &ioHandlerDatagramPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      __gnu_cxx::hash_map<uint16_t, IOHandlerDatagramPtr>::iterator iter = mDatagramMap.find(port);
      if (iter == mDatagramMap.end())
	return false;
      ioHandlerDatagramPtr = (*iter).second;
      return true;
    }

    bool RemoveHandler(struct sockaddr_in &addr) {
      boost::mutex::scoped_lock lock(mMutex);
      SockAddrMapT<IOHandlerAcceptPtr>::iterator aIter;
      SockAddrMapT<IOHandlerDataPtr>::iterator dIter;
      if ((dIter = mDataMap.find(addr)) != mDataMap.end())
	mDataMap.erase(dIter);
      else if ((aIter = mAcceptMap.find(addr)) != mAcceptMap.end())
	mAcceptMap.erase(aIter);
      else
	return false;
      return true;
    }

    void UnregisterAll() {
      boost::mutex::scoped_lock lock(mMutex);
      
      for (SockAddrMapT<IOHandlerDataPtr>::iterator dIter = mDataMap.begin(); dIter != mDataMap.end(); dIter++)
	(*dIter).second->Shutdown();

      for (SockAddrMapT<IOHandlerAcceptPtr>::iterator aIter = mAcceptMap.begin(); aIter != mAcceptMap.end(); aIter++)
	(*aIter).second->Shutdown();

      for (__gnu_cxx::hash_map<uint16_t, IOHandlerDatagramPtr>::iterator dgIter = mDatagramMap.begin(); dgIter != mDatagramMap.end(); dgIter++)
	(*dgIter).second->Shutdown();
    }

  private:
    boost::mutex                      mMutex;
    SockAddrMapT<IOHandlerDataPtr>    mDataMap;
    SockAddrMapT<IOHandlerAcceptPtr>  mAcceptMap;
    __gnu_cxx::hash_map<uint16_t, IOHandlerDatagramPtr>  mDatagramMap;
  };

}


#endif // HYPERTABLE_HANDLERMAP_H
