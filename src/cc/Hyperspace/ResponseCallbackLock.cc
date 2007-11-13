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

#include "ResponseCallbackLock.h"

using namespace Hyperspace;
using namespace Hypertable;

/**
 * 
 */
int ResponseCallbackLock::response(uint32_t status, uint64_t lockGeneration) {
  hbuilder_.initialize_from_request(m_event_ptr->header);
  CommBufPtr cbufPtr( new CommBuf(hbuilder_, 16) );
  cbufPtr->append_int(Error::OK);
  cbufPtr->append_int(status);
  cbufPtr->append_long(lockGeneration);
  return m_comm->send_response(m_event_ptr->addr, cbufPtr);
}

