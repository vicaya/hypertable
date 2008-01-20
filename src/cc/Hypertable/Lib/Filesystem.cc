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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include "AsyncComm/Serialization.h"

#include "Common/Error.h"

#include "Filesystem.h"

using namespace Hypertable;

int Filesystem::decode_response_open(EventPtr &event_ptr, int32_t *fdp) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    return Error::RESPONSE_TRUNCATED;

  if (error != Error::OK)
    return error;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)fdp))
    return Error::RESPONSE_TRUNCATED;

  return Error::OK;
}

/**
 */
int Filesystem::decode_response_create(EventPtr &event_ptr, int32_t *fdp) {
  return decode_response_open(event_ptr, fdp);
}


/**
 */
int Filesystem::decode_response_read(EventPtr &event_ptr, uint8_t *dst, uint32_t *nreadp) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  uint64_t offset;
  int error;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    return Error::RESPONSE_TRUNCATED;

  if (error != Error::OK)
    return error;

  if (!Serialization::decode_long(&msg, &remaining, &offset))
    return Error::RESPONSE_TRUNCATED;

  if (!Serialization::decode_int(&msg, &remaining, nreadp))
    return Error::RESPONSE_TRUNCATED;

  if (remaining < *nreadp)
    return Error::RESPONSE_TRUNCATED;

  // PERF: We could just send back a pointer to msg here
  memcpy(dst, msg, *nreadp);

  return Error::OK;
}


/**
 */
int Filesystem::decode_response_read_header(EventPtr &event_ptr, uint64_t *offsetp, uint32_t *amountp, uint8_t **dstp) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    return Error::RESPONSE_TRUNCATED;

  if (error != Error::OK)
    return error;

  if (!Serialization::decode_long(&msg, &remaining, offsetp))
    return Error::RESPONSE_TRUNCATED;

  if (!Serialization::decode_int(&msg, &remaining, amountp))
    return Error::RESPONSE_TRUNCATED;

  if (dstp)
    *dstp = msg;

  return Error::OK;
}



/**
 */
int Filesystem::decode_response_pread(EventPtr &event_ptr, uint8_t *dst, uint32_t *nreadp) {
  return decode_response_read(event_ptr, dst, nreadp);
}

/**
 */
int Filesystem::decode_response_append(EventPtr &event_ptr, uint64_t *offsetp, uint32_t *amountp) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    return Error::RESPONSE_TRUNCATED;

  if (error != Error::OK)
    return error;

  if (!Serialization::decode_long(&msg, &remaining, offsetp))
    return Error::RESPONSE_TRUNCATED;

  if (!Serialization::decode_int(&msg, &remaining, amountp))
    return Error::RESPONSE_TRUNCATED;

  return Error::OK;
}




/**
 */
int Filesystem::decode_response_length(EventPtr &event_ptr, int64_t *lenp) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    return Error::RESPONSE_TRUNCATED;

  if (error != Error::OK)
    return error;

  if (!Serialization::decode_long(&msg, &remaining, (uint64_t *)lenp))
    return Error::RESPONSE_TRUNCATED;

  return Error::OK;
}


/**
 */
int Filesystem::decode_response_readdir(EventPtr &event_ptr, std::vector<std::string> &listing) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error;
  uint32_t len;
  const char *str;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    return Error::RESPONSE_TRUNCATED;

  if (error != Error::OK)
    return error;

  listing.clear();
  if (!Serialization::decode_int(&msg, &remaining, &len))
    return Error::RESPONSE_TRUNCATED;

  for (uint32_t i=0; i<len; i++) {
    if (!Serialization::decode_string(&msg, &remaining, &str))
      return Error::RESPONSE_TRUNCATED;
    listing.push_back((std::string)str);
  }

  return Error::OK;
}


/**
 */
int Filesystem::decode_response(EventPtr &event_ptr) {
  uint8_t *msg = event_ptr->message;
  size_t remaining = event_ptr->messageLen;
  int error;

  if (!Serialization::decode_int(&msg, &remaining, (uint32_t *)&error))
    return Error::RESPONSE_TRUNCATED;

  return Error::OK;
}
