/** -*- C++ -*-
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

#ifndef HYPERTABLE_HEADERBUILDER_H
#define HYPERTABLE_HEADERBUILDER_H

#include <iostream>
using namespace std;

extern "C" {
#include <stdint.h>
}

#include "Common/atomic.h"

#include "Header.h"

namespace hypertable {

  class HeaderBuilder {

  public:

    static atomic_t ms_next_message_id;

    HeaderBuilder() : m_id(0), m_group_id(0), m_total_len(0), m_protocol(0), m_flags(0) {
      return;
    }

    HeaderBuilder(uint8_t protocol, uint32_t gid=0) : m_id(0), m_group_id(gid), m_total_len(0), m_protocol(protocol), m_flags(0) {
      return;
    }

    void initialize_from_request(Header::HeaderT *header) {
      m_id        = header->id;
      m_group_id  = header->gid;
      m_protocol  = header->protocol;
      m_flags     = header->flags;
      m_total_len = 0;
    }

    size_t header_length() {
      return sizeof(Header::HeaderT);
    }

    void encode(uint8_t **bufPtr) {
      Header::HeaderT *mheader;
      mheader = (Header::HeaderT *)*bufPtr;
      mheader->version = Header::VERSION;
      mheader->protocol = m_protocol;
      mheader->flags = m_flags;
      mheader->headerLen = sizeof(Header::HeaderT);
      mheader->id = m_id;
      mheader->gid = m_group_id;
      mheader->totalLen = m_total_len;
      (*bufPtr) += sizeof(Header::HeaderT);
    }

    uint32_t assign_unique_id() { m_id = atomic_inc_return(&ms_next_message_id); return m_id; }

    void set_flags(uint8_t flags) { m_flags = flags; }

    void add_flag(uint8_t flag) { m_flags |= flag; }

    void set_protocol(uint8_t protocol) { m_protocol = protocol; }

    void set_group_id(uint32_t groupId) { m_group_id = groupId; }

    void set_total_len(uint32_t totalLen) { m_total_len = totalLen; }

  protected:
    uint32_t  m_id;
    uint32_t  m_group_id;
    uint32_t  m_total_len;
    uint8_t   m_protocol;
    uint8_t   m_flags;
  };

}


#endif // HYPERTABLE_HEADERBUILDER_H
