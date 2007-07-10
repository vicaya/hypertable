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
