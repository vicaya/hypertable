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

#ifndef HYPERTABLE_MASTERFILEHANDLER_H
#define HYPERTABLE_MASTERFILEHANDLER_H

#include "AsyncComm/ApplicationQueue.h"

#include "Hyperspace/HandleCallback.h"

using namespace Hyperspace;

namespace hypertable {

  class RangeServer;

  /**
   * 
   */
  class MasterFileHandler : public HandleCallback {
  public:
    MasterFileHandler(RangeServer *rangeServer, ApplicationQueue *appQueue) : HandleCallback(EVENT_MASK_CHILD_NODE_ADDED), mRangeServer(rangeServer), mAppQueue(appQueue) { return; }
    virtual void ChildNodeAdded(std::string name) { return; }
    virtual void AttrSet(std::string name);
    virtual void AttrDel(std::string name) { return; }
    virtual void ChildNodeRemoved(std::string name) { return; }
    virtual void LockAcquired(uint32_t mode) { return; }
    virtual void LockReleased() { return; }
    RangeServer *mRangeServer;
    ApplicationQueue *mAppQueue;
  };
}

#endif // HYPERTABLE_MASTERFILEHANDLER_H
