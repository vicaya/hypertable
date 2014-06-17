/**
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
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

#ifndef HYPERSPACE_RESPONSECALLBACKREADDIRATTR_H
#define HYPERSPACE_RESPONSECALLBACKREADDIRATTR_H

#include "Common/Error.h"

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/ResponseCallback.h"

#include "DirEntryAttr.h"

namespace Hyperspace {

  class ResponseCallbackReaddirAttr : public Hypertable::ResponseCallback {
  public:
    ResponseCallbackReaddirAttr(Hypertable::Comm *comm,
                                Hypertable::EventPtr &event_ptr)
      : Hypertable::ResponseCallback(comm, event_ptr) { }

    int response(std::vector<DirEntryAttr> &listing);
  };

}

#endif // HYPERSPACE_RESPONSECALLBACKREADDIRATTR_H
