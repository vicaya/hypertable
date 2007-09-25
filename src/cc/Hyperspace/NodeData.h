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

#include <string>

#include <boost/thread/mutex.hpp>

#include "Common/ReferenceCount.h"

#include "HandleData.h"

namespace Hyperspace {

  class NodeData : public hypertable::ReferenceCount {
  public:
    NodeData() : fd(-1), ephemeral(false) { 
      return;
    }
    void AddHandle(uint64_t handle, HandleDataPtr &handlePtr) {
      boost::mutex::scoped_lock lock(mutex);
      handleMap[handle] = handlePtr;
    }
    bool RemoveHandle(uint64_t handle) {
      boost::mutex::scoped_lock lock(mutex);
      HandleMapT::iterator iter = handleMap.find(handle);
      if (iter != handleMap.end()) {
	handleMap.erase(iter);
	return true;
      }
      return false;
    }
    unsigned int ReferenceCount() {
      boost::mutex::scoped_lock lock(mutex);
      return handleMap.size();
    }
    int Close() {
      boost::mutex::scoped_lock lock(mutex);
      return close(fd);
    }

    boost::mutex mutex;
    std::string name;
    int         fd;
    bool        ephemeral;
    typedef __gnu_cxx::hash_map<uint64_t, HandleDataPtr> HandleMapT;
    HandleMapT handleMap;
  };
  typedef boost::intrusive_ptr<NodeData> NodeDataPtr;

}

#endif // HYPERSPACE_NODEDATA_H
