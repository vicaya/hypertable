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

#include "Common/Error.h"

#include "AsyncComm/CommBuf.h"

#include "ResponseCallbackReaddir.h"

using namespace Hyperspace;
using namespace Hypertable;

/**
 *
 */
int ResponseCallbackReaddir::response(std::vector<struct DirEntryT> &listing) {
  hbuilder_.initialize_from_request(m_event_ptr->header);
  uint32_t len = 8;

  for (size_t i=0; i<listing.size(); i++)
    len += encoded_length_dir_entry(listing[i]);

  CommBufPtr cbufPtr( new CommBuf(hbuilder_, len) );

  cbufPtr->append_int(Error::OK);
  cbufPtr->append_int(listing.size());
  
  for (size_t i=0; i<listing.size(); i++)
    encode_dir_entry(cbufPtr->get_data_ptr_address(), listing[i]);
  return m_comm->send_response(m_event_ptr->addr, cbufPtr);
}

