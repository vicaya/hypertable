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

    void AddRequest(uint32_t id, IOHandler *handler, DispatchHandler *dh, time_t expire) {
      mRequestCache.Insert(id, handler, dh, expire);
    }

    DispatchHandler *RemoveRequest(uint32_t id) {
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
