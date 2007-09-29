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
    virtual void AttrSet(std::string name) { cout << "ATTR SET " << name << endl << flush; }
    virtual void AttrDel(std::string name) { cout <<  "ATTR DEL " << name << endl << flush; }
    virtual void ChildNodeAdded(std::string name) { cout << "CHILD NODE ADDED " << name << endl << flush; }
    virtual void ChildNodeRemoved(std::string name) { cout << "CHILD NODE REMOVED " << name << endl << flush; }
    virtual void LockAcquired(uint32_t mode) { 
      if (mode == LOCK_MODE_SHARED)
	cout << "LOCK ACQUIRED shared" << endl << flush;
      else if (mode == LOCK_MODE_EXCLUSIVE)
	cout << "LOCK ACQUIRED exclusive" << endl << flush;
      else
	cout << "LOCK ACQUIRED " << mode << endl << flush;
    }
    virtual void LockReleased() { cout << "LOCK RELEASED" << endl << flush; }
  };

}

#endif // HYPERSPACE_FILEHANDLECALLBACK_H
