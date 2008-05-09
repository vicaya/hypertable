/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

int Filesystem::decode_response_open(EventPtr &event_ptr) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error, fd;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    throw Exception(Error::RESPONSE_TRUNCATED);

  if (error != Error::OK)
    throw Exception(error);

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&fd))
    throw Exception(Error::RESPONSE_TRUNCATED);

  return fd;
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
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  uint64_t offset;
  int error;
  uint32_t nread;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    throw Exception(Error::RESPONSE_TRUNCATED);

  if (error != Error::OK)
    throw Exception(error);

  if (!Serialization::decode_long(&msg, &remaining, &offset))
    throw Exception(Error::RESPONSE_TRUNCATED);

  if (!Serialization::decode_int(&msg, &remaining, &nread))
    throw Exception(Error::RESPONSE_TRUNCATED);

  if (remaining < nread || len < nread)
    throw Exception(Error::RESPONSE_TRUNCATED);

  // PERF: We could just send back a pointer to msg here
  memcpy(dst, msg, nread);

  return nread;
}


/**
 */
size_t
Filesystem::decode_response_read_header(EventPtr &event_ptr, uint64_t *offsetp,
                                        uint8_t **dstp) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error;
  uint32_t amount;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    throw Exception(Error::RESPONSE_TRUNCATED);

  if (error != Error::OK)
    throw Exception(error);

  if (!Serialization::decode_long(&msg, &remaining, offsetp))
    throw Exception(Error::RESPONSE_TRUNCATED);

  if (!Serialization::decode_int(&msg, &remaining, &amount))
    throw Exception(Error::RESPONSE_TRUNCATED);

  if (dstp)
    *dstp = msg;

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
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error;
  uint32_t amount;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    throw Exception(Error::RESPONSE_TRUNCATED);

  if (error != Error::OK)
    throw Exception(error);

  if (!Serialization::decode_long(&msg, &remaining, offsetp))
    throw Exception(Error::RESPONSE_TRUNCATED);

  if (!Serialization::decode_int(&msg, &remaining, &amount))
    throw Exception(Error::RESPONSE_TRUNCATED);

  return amount;
}


/**
 */
int64_t
Filesystem::decode_response_length(EventPtr &event_ptr) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error;
  int64_t len;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    throw Exception(Error::RESPONSE_TRUNCATED);

  if (error != Error::OK)
    throw Exception(error);

  if (!Serialization::decode_long(&msg, &remaining, (uint64_t *)&len))
    throw Exception(Error::RESPONSE_TRUNCATED);

  return len;
}


/**
 */
void
Filesystem::decode_response_readdir(EventPtr &event_ptr,
                                    std::vector<String> &listing) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error;
  uint32_t len;
  const char *str;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    throw Exception(Error::RESPONSE_TRUNCATED);

  if (error != Error::OK)
    throw Exception(error);

  listing.clear();
  if (!Serialization::decode_int(&msg, &remaining, &len))
    throw Exception(Error::RESPONSE_TRUNCATED);

  for (uint32_t i=0; i<len; i++) {
    if (!Serialization::decode_string(&msg, &remaining, &str))
      throw Exception(Error::RESPONSE_TRUNCATED);
    listing.push_back(str);
  }
}


/**
 */
bool
Filesystem::decode_response_exists(EventPtr &event_ptr) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error;
  bool exists;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    throw Exception(Error::RESPONSE_TRUNCATED);

  if (error != Error::OK)
    throw Exception(error);

  if (!Serialization::decode_bool(&msg, &remaining, &exists))
    throw Exception(Error::RESPONSE_TRUNCATED);

  return exists;
}



/**
 */
int
Filesystem::decode_response(EventPtr &event_ptr) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    throw Exception(Error::RESPONSE_TRUNCATED);

  return error;
}
