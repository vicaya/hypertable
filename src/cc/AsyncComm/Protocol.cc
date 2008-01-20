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


#include <cassert>
#include <iostream>

#include "Common/Error.h"
#include "Common/Logger.h"

#include "CommBuf.h"
#include "Event.h"
#include "Protocol.h"
#include "Serialization.h"

using namespace Hypertable;
using namespace std;

/**
 *
 */
int32_t Protocol::response_code(Event *event) {
  int32_t error;
  if (event->type == Event::ERROR)
    return event->error;
  uint8_t *msg = event->message;
  size_t remaining = event->messageLen;
  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    return Error::RESPONSE_TRUNCATED;
  return error;
}



/**
 *
 */
std::string Protocol::string_format_message(Event *event) {
  int error = 0;
  uint8_t *msg = event->message;
  size_t remaining = event->messageLen;

  if (event == 0)
    return (std::string)"NULL event";

  if (event->type != Event::MESSAGE)
    return event->toString();

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    return (std::string)"Message Truncated";

  if (error == Error::OK)
    return (std::string)Error::get_text(error);
  else {
    const char *str;

    if (!Serialization::decode_string(&msg, &remaining, &str))
      return (std::string)Error::get_text(error) + " - truncated";

    return (std::string)Error::get_text(error) + " : " + str;
  }
}



/**
 *
 */
CommBuf *Protocol::create_error_message(HeaderBuilder &hbuilder, int error, const char *msg) {
  CommBuf *cbuf = new CommBuf(hbuilder, 4 + Serialization::encoded_length_string(msg));
  cbuf->append_int(error);
  cbuf->append_string(msg);
  return cbuf;
}


