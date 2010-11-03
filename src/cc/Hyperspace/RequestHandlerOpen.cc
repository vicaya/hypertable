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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/Logger.h"

#include "AsyncComm/ResponseCallback.h"
#include "Common/Serialization.h"

#include "Hypertable/Lib/Types.h"

#include "Master.h"
#include "Protocol.h"
#include "RequestHandlerOpen.h"

using namespace Hyperspace;
using namespace Hypertable;
using namespace Serialization;

void RequestHandlerOpen::run() {
  ResponseCallbackOpen cb(m_comm, m_event_ptr);
  size_t decode_remain = m_event_ptr->payload_len;
  const uint8_t *decode_ptr = m_event_ptr->payload;
  Attribute attr;
  std::vector<Attribute> attrs;

  try {
    uint32_t flags = decode_i32(&decode_ptr, &decode_remain);
    uint32_t event_mask = decode_i32(&decode_ptr, &decode_remain);
    const char *name = decode_vstr(&decode_ptr, &decode_remain);
    uint32_t attr_count = decode_i32(&decode_ptr, &decode_remain);

    while (attr_count--) {
      attr.name = decode_vstr(&decode_ptr, &decode_remain);
      attr.value = decode_vstr(&decode_ptr, &decode_remain, &attr.value_len);
      attrs.push_back(attr);
    }
    m_master->open(&cb, m_session_id, name, flags, event_mask, attrs);
  }
  catch (Exception &e) {
    if (   e.code() == Error::HYPERSPACE_BAD_PATHNAME
        || e.code() == Error::HYPERSPACE_FILE_NOT_FOUND) // warning should be sufficient
      HT_WARN_OUT << e << HT_END;
    else
      HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling Hyperspace open");
  }
}
