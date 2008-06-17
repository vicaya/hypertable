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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERSPACE_NODEDATA_H
#define HYPERSPACE_NODEDATA_H

#include <iostream>
#include <list>

#include <boost/thread/mutex.hpp>

#include "Common/String.h"
#include "Common/HashMap.h"
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
    NodeData() : ephemeral(false), cur_lock_mode(0), lock_generation(0),
                 exclusive_lock_handle(0) { }
    void add_handle(uint64_t handle, HandleDataPtr &handle_data) {
      handle_map[handle] = handle_data;
    }

    bool remove_handle(uint64_t handle) {
      HandleMap::iterator iter = handle_map.find(handle);
      if (iter != handle_map.end()) {
        handle_map.erase(iter);
        return true;
      }
      return false;
    }

    unsigned int reference_count() {
      return handle_map.size();
    }

    typedef hash_map<uint64_t, HandleDataPtr> HandleMap;
    typedef std::set<uint64_t> LockHandleSet;
    typedef std::list<LockRequest> LockRequestList;

    boost::mutex mutex;
    String      name;
    bool        ephemeral;
    HandleMap   handle_map;
    uint32_t    cur_lock_mode;
    uint64_t    lock_generation;
    LockHandleSet shared_lock_handles;
    uint64_t    exclusive_lock_handle;
    LockRequestList pending_lock_reqs;
  };

  typedef boost::intrusive_ptr<NodeData> NodeDataPtr;

}

#endif // HYPERSPACE_NODEDATA_H
