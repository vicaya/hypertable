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

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/Serialization.h"

#include "AsyncComm/CommBuf.h"

#include "ResponseCallbackAttrList.h"

using namespace std;
using namespace Hyperspace;
using namespace Hypertable;

/**
 *
 */
int ResponseCallbackAttrList::response(const vector<string> &anames) {
  CommHeader header;
  header.initialize_from_request_header(m_event_ptr->header);
  size_t payloadsize = 8;
  vector<string>::const_iterator it;

  for (it=anames.begin(); it != anames.end(); ++it) {
    payloadsize += Serialization::encoded_length_vstr(*it);
  }

  CommBufPtr cbp(new CommBuf(header, payloadsize));
  cbp->append_i32(Error::OK);
  cbp->append_i32(anames.size());

  for (it=anames.begin(); it != anames.end(); ++it) {
    cbp->append_vstr(*it);
  }

  return m_comm->send_response(m_event_ptr->addr, cbp);
}

