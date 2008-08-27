/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_RESPONSECALLBACKGETSCHEMA_H
#define HYPERTABLE_RESPONSECALLBACKGETSCHEMA_H

#include "Common/Error.h"

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/ResponseCallback.h"

namespace Hypertable {

  class ResponseCallbackGetSchema : public ResponseCallback {
  public:
    ResponseCallbackGetSchema(Comm *comm, EventPtr &event_ptr) : ResponseCallback(comm, event_ptr) { return; }
    int response(const char *schema);
  };

}


#endif // HYPERTABLE_RESPONSECALLBACKGETSCHEMA_H
