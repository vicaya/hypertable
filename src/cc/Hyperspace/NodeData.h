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

#ifndef HYPERSPACE_NODEDATA_H
#define HYPERSPACE_NODEDATA_H

#include <iostream>
#include <list>

#include <string>

#include <boost/thread/mutex.hpp>

#include "Common/ReferenceCount.h"

#include "HandleData.h"

namespace Hyperspace {

  class LockRequest {
  public:
    LockRequest(uint64_t h, int m) : handle(h), mode(m) { return; }
    uint64_t handle;
    int mode;
  };

  class NodeData : public Hypertable::ReferenceCount {
  public:
    NodeData() : fd(-1), ephemeral(false), currentLockMode(0), lockGeneration(0), exclusiveLockHandle(0) { 
      return;
    }
    void add_handle(uint64_t handle, HandleDataPtr &handlePtr) {
      handleMap[handle] = handlePtr;
    }

    bool remove_handle(uint64_t handle) {
      HandleMapT::iterator iter = handleMap.find(handle);
      if (iter != handleMap.end()) {
	handleMap.erase(iter);
	return true;
      }
      return false;
    }

    unsigned int reference_count() {
      return handleMap.size();
    }

    void close() {
      if (fd != -1) {
	::close(fd);
	fd = -1;
      }
    }

    boost::mutex mutex;
    std::string name;
    int         fd;
    bool        ephemeral;
    typedef __gnu_cxx::hash_map<uint64_t, HandleDataPtr> HandleMapT;
    HandleMapT handleMap;
    uint32_t currentLockMode;
    uint64_t lockGeneration;
    set<uint64_t> sharedLockHandles;
    uint64_t exclusiveLockHandle;
    list<LockRequest> pendingLockRequests;
  };
  typedef boost::intrusive_ptr<NodeData> NodeDataPtr;

}

#endif // HYPERSPACE_NODEDATA_H
