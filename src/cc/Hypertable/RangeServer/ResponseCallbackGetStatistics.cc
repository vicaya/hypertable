/** -*- c++ -*-
 * Copyright (C) 2008 Donald <donaldliew@gmail.com>
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

#include "Common/Compat.h"
#include "ResponseCallbackGetStatistics.h"

using namespace Hypertable;

int ResponseCallbackGetStatistics::response(StaticBuffer &ext) {
  CommHeader header;
  header.initialize_from_request_header(m_event_ptr->header);
  CommBufPtr cbp(new CommBuf( header, 4, ext));
  cbp->append_i32(Error::OK);
  return m_comm->send_response(m_event_ptr->addr, cbp);
}
