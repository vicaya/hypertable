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

#ifndef HYPERSPACE_HANDLEDATA_H
#define HYPERSPACE_HANDLEDATA_H

#include <string>

#include "Common/ReferenceCount.h"

#include "SessionData.h"

namespace Hyperspace {

  class NodeData;

  class HandleData : public Hypertable::ReferenceCount {
  public:
    std::string  name;
    uint64_t     id;
    uint32_t     openFlags;
    uint32_t     eventMask;
    NodeData    *node;
    SessionDataPtr sessionPtr;
    bool         locked;
  };
  typedef boost::intrusive_ptr<HandleData> HandleDataPtr;

}

#endif // HYPERSPACE_HANDLEDATA_H
