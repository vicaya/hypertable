/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (vents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#include "Common/Compat.h"
#include "Common/Error.h"

#include "AsyncComm/CommBuf.h"
#include "Common/Serialization.h"

#include "ResponseCallbackRegisterServer.h"

using namespace Hypertable;
using namespace Serialization;

int ResponseCallbackRegisterServer::response(const String &location) {
  CommHeader header;
  header.initialize_from_request_header(m_event_ptr->header);
  CommBufPtr cbp(new CommBuf(header, 4 + encoded_length_vstr(location)));
  cbp->append_i32(Error::OK);
  cbp->append_vstr(location);
  return m_comm->send_response(m_event_ptr->addr, cbp);
}
