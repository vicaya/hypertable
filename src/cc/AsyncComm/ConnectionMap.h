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


//#define DISABLE_LOG_DEBUG

#include <boost/thread/mutex.hpp>

#include "Common/Logger.h"
#include "Common/SockAddrMap.h"

#include "IOHandlerData.h"

namespace hypertable {

  class ConnectionMap {

  public:

    bool Lookup(struct sockaddr_in &addr, IOHandlerDataPtr &ioHandlerDataPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      SockAddrMapT<IOHandlerDataPtr>::iterator iter = mMap.find(addr);
      if (iter == mMap.end())
	return false;
      ioHandlerDataPtr = (*iter).second;
      return true;
    }

    void Insert(IOHandlerDataPtr &ioHandlerDataPtr) {
      boost::mutex::scoped_lock lock(mMutex);
      mMap[ioHandlerDataPtr->GetAddress()] = ioHandlerDataPtr;
    }

    void Remove(struct sockaddr_in &addr) {
      boost::mutex::scoped_lock lock(mMutex);
      LOG_ENTER;
      SockAddrMapT<IOHandlerDataPtr>::iterator iter = mMap.find(addr);
      assert(iter != mMap.end());
      mMap.erase(iter);
    }

  private:
    SockAddrMapT<IOHandlerDataPtr>  mMap;
    boost::mutex                    mMutex;
  };

}
