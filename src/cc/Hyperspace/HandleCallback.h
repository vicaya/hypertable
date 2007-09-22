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

#ifndef HYPERSPACE_HANDLECALLBACK_H
#define HYPERSPACE_HANDLECALLBACK_H

#include "Common/ReferenceCount.h"

namespace Hyperspace {

  /**
   * The following event masks are ORed together and
   * passed in as the eventMask argument to Open()
   * to indicate which events should be reported to
   * the application for the opened handle.
   */
  enum {
    EVENT_MASK_ATTR_MODIFIED      = 0x0001,
    EVENT_MASK_CHILD_NODE_CHANGE  = 0x0002,
    EVENT_MASK_LOCK_ACQUIRED      = 0x0004,
    EVENT_MASK_HANDLE_INVALIDATED = 0x0008,
    EVENT_MASK_CONFLICTING_LOCK   = 0x0010,
  };

  /**
   * A callback object derived from this class gets passed
   * into each Open() call.  Node state changes get reported
   * to the application via this callback.
   */
  class HandleCallback : public hypertable::ReferenceCount {
  public:
    HandleCallback(uint32_t eventMask) : mEventMask(eventMask) { return; }
    virtual void AttrModified(std::string name) = 0;
    virtual void ChildNodeChange() = 0;
    virtual void LockAcquired() = 0;
    virtual void HandleInvalidated() = 0;
    virtual void ConflictingLockRequest() = 0;
    int GetEventMask() { return mEventMask; }
  protected:
    uint32_t mEventMask;
  };
  typedef boost::intrusive_ptr<HandleCallback> HandleCallbackPtr;
}

#endif // HYPERSPACE_HANDLECALLBACK_H
