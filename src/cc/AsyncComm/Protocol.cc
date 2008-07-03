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

#include <cassert>
#include <iostream>

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Serialization.h"

#include "CommBuf.h"
#include "Event.h"
#include "Protocol.h"

using namespace Hypertable;
using namespace Serialization;

/**
 *
 */
int32_t Protocol::response_code(Event *event) {
  if (event->type == Event::ERROR)
    return event->error;

  const uint8_t *msg = event->message;
  size_t remaining = event->message_len;

  try { return decode_i32(&msg, &remaining); }
  catch (Exception &e) { return e.code(); }
}



/**
 *
 */
String Protocol::string_format_message(Event *event) {
  int error = 0;
  const uint8_t *msg = event->message;
  size_t remaining = event->message_len;

  if (event == 0)
    return String("NULL event");

  if (event->type != Event::MESSAGE)
    return event->to_str();

  try {
    error = decode_i32(&msg, &remaining);

    if (error == Error::OK)
      return Error::get_text(error);

    uint16_t len;
    const char *str = decode_str16(&msg, &remaining, &len);

    return String(str, len > 150 ? 150 : len);
  }
  catch (Exception &e) {
    return format("%s - %s", e.what(), Error::get_text(e.code()));
  }
}



/**
 *
 */
CommBuf *
Protocol::create_error_message(HeaderBuilder &hbuilder, int error,
                               const char *msg) {
  CommBuf *cbuf = new CommBuf(hbuilder, 4 + encoded_length_str16(msg));
  cbuf->append_i32(error);
  cbuf->append_str16(msg);
  return cbuf;
}


