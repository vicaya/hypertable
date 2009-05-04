/**
 * Copyright (C) 2009 Mateusz Berezecki
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

#ifndef HYPERSPACE_RESPONSECALLBACKATTRLIST_H
#define HYPERSPACE_RESPONSECALLBACKATTRLIST_H

#include "Common/Error.h"

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/ResponseCallback.h"

#include "Common/StaticBuffer.h"

#include <vector>

namespace Hyperspace {

  using namespace std;
  class ResponseCallbackAttrList : public Hypertable::ResponseCallback {
  public:
    ResponseCallbackAttrList(Hypertable::Comm *comm,
                            Hypertable::EventPtr &event_ptr)
      : Hypertable::ResponseCallback(comm, event_ptr) { }

    int response(const vector<string> &attributes);
  };

}

#endif // HYPERSPACE_RESPONSECALLBACKATTRLIST_H
