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

#ifndef HYPERSPACE_FILEHANDLECALLBACK_H
#define HYPERSPACE_FILEHANDLECALLBACK_H

#include "Hyperspace/HandleCallback.h"

namespace Hyperspace {

  class FileHandleCallback : public HandleCallback {
  public:
    FileHandleCallback(uint32_t eventMask) : HandleCallback(eventMask) { return; }
    virtual void AttrModified(std::string name) { cout << endl << "ATTR MODIFIED " << name; }
    virtual void ChildNodeChange() { cout << endl << "CHILD NODE CHANGED"; }
    virtual void LockAcquired() { cout << endl << "LOCK ACQUIRED"; }
    virtual void HandleInvalidated() { cout << endl << "HANDLE INVALIDATED"; }
    virtual void ConflictingLockRequest() { cout << endl << "CONFLICTING LOCK REQUEST"; }
  };

}

#endif // HYPERSPACE_FILEHANDLECALLBACK_H
