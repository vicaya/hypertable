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


#ifndef DISPATCHHANDLERSYNCHRONIZER_H
#define DISPATCHHANDLERSYNCHRONIZER_H

#include <queue>

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "DispatchHandler.h"
#include "Event.h"

using namespace std;

namespace hypertable {

  class DispatchHandlerSynchronizer : public DispatchHandler {
  
  public:
    DispatchHandlerSynchronizer();
    virtual void handle(EventPtr &eventPtr);
    bool WaitForReply(EventPtr &eventPtr);

    /**
     * @deprecated
     */
    bool WaitForReply(EventPtr &eventPtr, uint32_t id);

  private:
    queue<EventPtr>   mReceiveQueue;
    boost::mutex      mMutex;
    boost::condition  mCond;
  };
}


#endif // DISPATCHHANDLERSYNCHRONIZER_H
