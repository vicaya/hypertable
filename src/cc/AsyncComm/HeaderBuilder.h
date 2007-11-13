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

namespace Hypertable {

  /**
   * Builds a response or request header for messages sent over the Comm layer.
   * An object of this type gets passed into the CommBuf constructor and is used to generate
   * the message header.
   *
   * <p><b>NOTE:</b> The reason for this class is that someday it will support a recipient list
   * which will be a variable sized set of addresses that the message should be delivered to.  By
   * encapsulating the construction of message headers into a class like this one, variable sized
   * headers can be easily supported.
   */
  class HeaderBuilder {

  public:

    /** Constructor.  Initializes all members to 0.
     */
    HeaderBuilder() : m_id(0), m_group_id(0), m_total_len(0), m_protocol(0), m_flags(0) {
      return;
    }

    /** Constructor.  Initializes the m_protocol and m_group_id members with the
     * supplied arguments, all other members are set to 0.
     *
     * @param protocol application protocol, can be one of PROTOCOL_NONE, PROTOCOL_DFSBROKER,
     *                 PROTOCOL_HYPERSPACE, PROTOCOL_HYPERTABLE_MASTER,
     *                 PROTOCOL_HYPERTABLE_RANGESERVER (see Header)
     * @param gid the group ID.  If the server is using an ApplicationQueue, then request
     *            messages with the same group ID will get carried out in series
     */
    HeaderBuilder(uint8_t protocol, uint32_t gid=0) : m_id(0), m_group_id(gid), m_total_len(0), m_protocol(protocol), m_flags(0) {
      return;
    }

    /** This method is used to initialize a response header from a give request header.  It
     * pulls the ID, the group ID, and the flags from the given request header.
     *
     * @param header pointer to the request message header
     */
    void initialize_from_request(Header::HeaderT *header) {
      m_id        = header->id;
      m_group_id  = header->gid;
      m_protocol  = header->protocol;
      m_flags     = header->flags;
      m_total_len = 0;
    }

    /** Returns the length of the header that would be generated */
    size_t header_length() { return sizeof(Header::HeaderT); }

    /** Encodes the header to the given buffer.  Advances the buffer pointer by the
     * length of the header written.
     *
     * @param bufPtr address of buffer pointer to write header to
     */
    void encode(uint8_t **bufPtr);

    /** Sets the flags member of the header builder.  Currently this is only
     * used internally by the Comm layer and the 0th bit is a flag that
     * indicates whether the message is a request or a response.
     *
     * @param flags new set of flags
     */
    void set_flags(uint8_t flags) { m_flags = flags; }

    /** ORs in some flag bits to the existing flags 
     *
     * @param flags flags to OR into the existing set
     */
    void add_flag(uint8_t flags) { m_flags |= flags; }

    /** Sets the protocol family of the message
     *
     * @param protocol protocol family
     */
    void set_protocol(uint8_t protocol) { m_protocol = protocol; }

    /** Sets the group ID of the message
     *
     * @param group_id the group ID of the message
     */
    void set_group_id(uint32_t group_id) { m_group_id = group_id; }

    /** Sets the total length of the message.
     *
     * @param total_len total length of message (including header)
     */
    void set_total_len(uint32_t total_len) { m_total_len = total_len; }

  protected:
    uint32_t  m_id;
    uint32_t  m_group_id;
    uint32_t  m_total_len;
    uint8_t   m_protocol;
    uint8_t   m_flags;
  };

}


#endif // HYPERTABLE_HEADERBUILDER_H
