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
    virtual void AttrSet(std::string name) { cout << endl << "ATTR SET " << name << flush; }
    virtual void AttrDel(std::string name) { cout << endl << "ATTR DEL " << name << flush; }
    virtual void ChildNodeAdded(std::string name) { cout << endl << "CHILD NODE ADDED " << name << flush; }
    virtual void ChildNodeRemoved(std::string name) { cout << endl << "CHILD NODE REMOVED " << name << flush; }
    virtual void LockAcquired(uint32_t mode) { 
      if (mode == LOCK_MODE_SHARED)
	cout << endl << "LOCK ACQUIRED shared" << flush;
      else if (mode == LOCK_MODE_EXCLUSIVE)
	cout << endl << "LOCK ACQUIRED exclusive" << flush;
      else
	cout << endl << "LOCK ACQUIRED " << mode << flush;
    }
    virtual void LockReleased() { cout << endl << "LOCK RELEASED" << flush; }
  };

}

#endif // HYPERSPACE_FILEHANDLECALLBACK_H
