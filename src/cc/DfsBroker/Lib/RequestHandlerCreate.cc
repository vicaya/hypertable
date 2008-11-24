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
#include "Common/Logger.h"

#include "AsyncComm/ResponseCallback.h"
#include "Common/Serialization.h"

#include "RequestHandlerCreate.h"

using namespace Hypertable;
using namespace DfsBroker;
using namespace Serialization;

/**
 *
 */
void RequestHandlerCreate::run() {
  ResponseCallbackOpen cb(m_comm, m_event_ptr);
  const uint8_t *decode_ptr = m_event_ptr->payload;
  size_t decode_remain = m_event_ptr->payload_len;

  try {
    bool overwrite = decode_i16(&decode_ptr, &decode_remain);
    int32_t replication = decode_i32(&decode_ptr, &decode_remain);
    int32_t bufsz = decode_i32(&decode_ptr, &decode_remain);
    int64_t blksz = decode_i64(&decode_ptr, &decode_remain);
    const char *fname = decode_str16(&decode_ptr, &decode_remain);

    // validate filename
    if (fname[strlen(fname)-1] == '/')
      HT_THROWF(Error::DFSBROKER_BAD_FILENAME, "bad filename: %s", fname);

    m_broker->create(&cb, fname, overwrite, bufsz, replication,
                     blksz);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    cb.error(e.code(), "Error handling CREATE message");
  }
}
