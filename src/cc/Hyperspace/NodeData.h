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

#include <string>

#include "Common/ReferenceCount.h"

namespace Hyperspace {

  class NodeData : public hypertable::ReferenceCount {
  public:
    NodeData() : fd(-1) { return; }
    std::string name;
    int         fd;
  };
  typedef boost::intrusive_ptr<NodeData> NodeDataPtr;

}

#endif // HYPERSPACE_NODEDATA_H
