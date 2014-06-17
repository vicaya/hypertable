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

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/ResponseCallback.h"
#include "Common/Serialization.h"

#include "Master.h"
#include "RequestHandlerReadpathAttr.h"
#include "ResponseCallbackReadpathAttr.h"

using namespace Hyperspace;
using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
void RequestHandlerReadpathAttr::run() {
  ResponseCallbackReadpathAttr cb(m_comm, m_event_ptr);
  size_t decode_remain = m_event_ptr->payload_len;
  const uint8_t *decode_ptr = m_event_ptr->payload;

  try {
    uint64_t handle = decode_i64(&decode_ptr, &decode_remain);
    const char *name = decode_vstr(&decode_ptr, &decode_remain);

    m_master->readpath_attr(&cb, m_session_id, handle, name);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling READPATHATTR message");
  }
}
