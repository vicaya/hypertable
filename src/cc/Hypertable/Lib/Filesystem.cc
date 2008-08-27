/** -*- c++ -*-
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
#include "Common/Error.h"
#include "Common/Serialization.h"

#include "Filesystem.h"

using namespace Hypertable;
using namespace Serialization;

int Filesystem::decode_response_open(EventPtr &event_ptr) {
  const uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->message_len;
  int error = decode_i32(&msg, &remaining);

  if (error != Error::OK)
    HT_THROW(error, "");

  return decode_i32(&msg, &remaining);
}


/**
 */
int Filesystem::decode_response_create(EventPtr &event_ptr) {
  return decode_response_open(event_ptr);
}


/**
 */
size_t
Filesystem::decode_response_read(EventPtr &event_ptr, void *dst, size_t len) {
  const uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->message_len;

  int error = decode_i32(&msg, &remaining);

  if (error != Error::OK)
    HT_THROW(error, "");

  // uint64_t offset =
  decode_i64(&msg, &remaining);
  uint32_t nread = decode_i32(&msg, &remaining);

  if (nread == (uint32_t)-1)
    return 0;

  if (remaining < nread)
    HT_THROW(Error::RESPONSE_TRUNCATED, "");

  // PERF: We could just send back a pointer to msg here
  memcpy(dst, msg, nread);

  return nread;
}


/**
 */
size_t
Filesystem::decode_response_read_header(EventPtr &event_ptr, uint64_t *offsetp,
                                        uint8_t **dstp) {
  const uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->message_len;

  int error = decode_i32(&msg, &remaining);

  if (error != Error::OK)
    HT_THROW(error, "");

  *offsetp = decode_i64(&msg, &remaining);
  uint32_t amount = decode_i32(&msg, &remaining);

  if (dstp)
    *dstp = (uint8_t *)msg;

  return amount;
}


/**
 */
size_t
Filesystem::decode_response_pread(EventPtr &event_ptr, void *dst, size_t len) {
  return decode_response_read(event_ptr, dst, len);
}


/**
 */
size_t
Filesystem::decode_response_append(EventPtr &event_ptr, uint64_t *offsetp) {
  const uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->message_len;

  int error = decode_i32(&msg, &remaining);

  if (error != Error::OK)
    HT_THROW(error, "");

  *offsetp = decode_i64(&msg, &remaining);

  return decode_i32(&msg, &remaining);
}


/**
 */
int64_t
Filesystem::decode_response_length(EventPtr &event_ptr) {
  const uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->message_len;

  int error = decode_i32(&msg, &remaining);

  if (error != Error::OK)
    HT_THROW(error, "");

  return decode_i64(&msg, &remaining);
}


/**
 */
void
Filesystem::decode_response_readdir(EventPtr &event_ptr,
                                    std::vector<String> &listing) {
  const uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->message_len;

  int error = decode_i32(&msg, &remaining);

  if (error != Error::OK)
    HT_THROW(error, "");

  listing.clear();

  uint32_t len = decode_i32(&msg, &remaining);
  uint16_t slen;

  for (uint32_t i=0; i<len; i++) {
    const char *str = decode_str16(&msg, &remaining, &slen);
    listing.push_back(String(str, slen));
  }
}


/**
 */
bool
Filesystem::decode_response_exists(EventPtr &event_ptr) {
  const uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->message_len;

  int error = decode_i32(&msg, &remaining);

  if (error != Error::OK)
    HT_THROW(error, "");

  return decode_bool(&msg, &remaining);
}



/**
 */
int
Filesystem::decode_response(EventPtr &event_ptr) {
  const uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->message_len;

  return decode_i32(&msg, &remaining);
}
