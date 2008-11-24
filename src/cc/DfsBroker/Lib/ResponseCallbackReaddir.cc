/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include "Common/Compat.h"
#include "Common/Error.h"

#include "AsyncComm/CommBuf.h"
#include "Common/Serialization.h"

#include "ResponseCallbackReaddir.h"

using namespace Hypertable;
using namespace DfsBroker;
using namespace Serialization;

int ResponseCallbackReaddir::response(std::vector<std::string> &listing) {
  CommHeader header;
  header.initialize_from_request_header(m_event_ptr->header);
  uint32_t len = 8;
  for (size_t i=0; i<listing.size(); i++)
    len += encoded_length_str16(listing[i]);
  CommBufPtr cbp( new CommBuf(header, len) );
  cbp->append_i32(Error::OK);
  cbp->append_i32(listing.size());
  for (size_t i=0; i<listing.size(); i++)
    cbp->append_str16(listing[i]);
  return m_comm->send_response(m_event_ptr->addr, cbp);
}
