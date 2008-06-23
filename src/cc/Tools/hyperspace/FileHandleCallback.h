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

#ifndef HYPERSPACE_FILEHANDLECALLBACK_H
#define HYPERSPACE_FILEHANDLECALLBACK_H

#include "Hyperspace/HandleCallback.h"

namespace Hyperspace {

  class FileHandleCallback : public HandleCallback {
  public:
    FileHandleCallback(uint32_t event_mask) : HandleCallback(event_mask) { return; }
    virtual void attr_set(const std::string &name) { std::cout << "ATTR SET " << name << std::endl; }
    virtual void attr_del(const std::string &name) { std::cout <<  "ATTR DEL " << name << std::endl; }
    virtual void child_node_added(const std::string &name) { std::cout << "CHILD NODE ADDED " << name << std::endl; }
    virtual void child_node_removed(const std::string &name) { std::cout << "CHILD NODE REMOVED " << name << std::endl; }
    virtual void lock_acquired(uint32_t mode) {
      if (mode == LOCK_MODE_SHARED)
        std::cout << "LOCK ACQUIRED shared" << std::endl;
      else if (mode == LOCK_MODE_EXCLUSIVE)
        std::cout << "LOCK ACQUIRED exclusive" << std::endl;
      else
        std::cout << "LOCK ACQUIRED " << mode << std::endl;
    }
    virtual void lock_released() { std::cout << "LOCK RELEASED" << std::endl; }
  };

}

#endif // HYPERSPACE_FILEHANDLECALLBACK_H
