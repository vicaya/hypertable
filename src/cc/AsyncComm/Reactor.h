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

#ifndef HYPERTABLE_REACTOR_H
#define HYPERTABLE_REACTOR_H

#include <queue>

#include <boost/thread/thread.hpp>

#include "PollTimeout.h"
#include "RequestCache.h"
#include "Timer.h"

namespace hypertable {

  class Reactor {

    friend class ReactorFactory;

  public:

    static const int READ_READY;
    static const int WRITE_READY;

    Reactor();

    void operator()();

    void AddRequest(uint32_t id, IOHandler *handler, DispatchHandler *dh, boost::xtime &expire) {
      boost::mutex::scoped_lock lock(mMutex);
      mRequestCache.Insert(id, handler, dh, expire);
    }

    DispatchHandler *RemoveRequest(uint32_t id) {
      boost::mutex::scoped_lock lock(mMutex);
      return mRequestCache.Remove(id);
    }

    void CancelRequests(IOHandler *handler) {
      boost::mutex::scoped_lock lock(mMutex);
      mRequestCache.PurgeRequests(handler);
    }

    void AddTimer(struct TimerT &timer) {
      boost::mutex::scoped_lock lock(mMutex);
      mTimerHeap.push(timer);
      PollLoopInterrupt();
    }

    void HandleTimeouts(PollTimeout &nextTimeout);

#if defined(__linux__)    
    int pollFd;
#elif defined (__APPLE__)
    int kQueue;
#endif

  protected:

    void PollLoopInterrupt();
    void PollLoopContinue();

    boost::mutex    mMutex;
    boost::thread   *threadPtr;
    RequestCache    mRequestCache;
    std::priority_queue<TimerT, std::vector<TimerT>, ltTimer> mTimerHeap;
    int             mInterruptSd;
    bool            mInterruptInProgress;
  };

}

#endif // HYPERTABLE_REACTOR_H
