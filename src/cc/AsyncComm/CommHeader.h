/** -*- c++ -*-
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

#ifndef HYPERTABLE_COMMHEADER_H
#define HYPERTABLE_COMMHEADER_H

namespace Hypertable {

  class CommHeader {

  public:

    static const uint8_t VERSION = 1;

    static const size_t FIXED_LENGTH = 36;

    static const uint16_t FLAGS_BIT_REQUEST          = 0x0001;
    static const uint16_t FLAGS_BIT_IGNORE_RESPONSE  = 0x0002;
    static const uint16_t FLAGS_BIT_URGENT           = 0x0004;
    static const uint16_t FLAGS_BIT_PAYLOAD_CHECKSUM = 0x8000;

    static const uint16_t FLAGS_MASK_REQUEST          = 0xFFFE;
    static const uint16_t FLAGS_MASK_IGNORE_RESPONSE  = 0xFFFD;
    static const uint16_t FLAGS_MASK_URGENT           = 0xFFFB;
    static const uint16_t FLAGS_MASK_PAYLOAD_CHECKSUM = 0x7FFF;

    CommHeader()
      : version(1), header_len(FIXED_LENGTH), flags(0),
        header_checksum(0), id(0), gid(0), total_len(0),
        timeout_ms(0), payload_checksum(0), command(0) {  }

    CommHeader(uint64_t cmd, uint32_t timeout=0)
      : version(1), header_len(FIXED_LENGTH), flags(0),
        header_checksum(0), id(0), gid(0), total_len(0),
        timeout_ms(timeout), payload_checksum(0),
        command(cmd) {  }

    size_t fixed_length() const { return FIXED_LENGTH; }
    size_t encoded_length() const { return FIXED_LENGTH; }
    void encode(uint8_t **bufp);
    void decode(const uint8_t **bufp, size_t *remainp);

    void set_total_length(uint32_t len) { total_len = len; }

    void initialize_from_request_header(CommHeader &req_header) {
      flags = req_header.flags;
      id = req_header.id;
      gid = req_header.gid;
      command = req_header.command;
      total_len = 0;
    }

    uint8_t version;
    uint8_t header_len;
    uint16_t flags;
    uint32_t header_checksum;
    uint32_t id;
    uint32_t gid;
    uint32_t total_len;
    uint32_t timeout_ms;
    uint32_t payload_checksum;
    uint64_t command;
  };

}

#endif // HYPERTABLE_COMMHEADER_H
