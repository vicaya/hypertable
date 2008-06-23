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

#ifndef HYPERTABLE_COMMBUF_H
#define HYPERTABLE_COMMBUF_H

#include <string>

#include <boost/shared_ptr.hpp>

#include "Common/ByteString.h"
#include "Common/Logger.h"
#include "Common/Serialization.h"
#include "Common/StaticBuffer.h"

#include "HeaderBuilder.h"

namespace Hypertable {

  /**
   * Message buffer sent over the network
   * by the AsyncComm subsystem.  It consists of a primary
   * buffer and an extended buffer and also contains buffer pointers
   * that keep track of how much data has already been written
   * in the case of partial writes.  These pointers are managed by
   * the AsyncComm subsytem iteslf.  The following example
   * illustrates how to build a request message using the
   * CommBuf.
   *
   * <pre>
   *   HeaderBuilder hbuilder(Header::PROTOCOL_DFSBROKER);
   *   CommBuf *cbuf = new CommBuf(hbuilder, 2);
   *   cbuf->append_i16(COMMAND_STATUS);
   * </pre>
   *
   * The following is a real world example of a CommBuf being used
   * to send back a response from a read request.
   *
   * <pre>
   *   hbuilder.initialize_from_request(m_event_ptr->header);
   *   CommBufPtr cbp(new CommBuf(hbuilder, 16, data, nread));
   *   cbp->append_i32(Error::OK);
   *   cbp->append_i64(offset);
   *   cbp->append_i32(nread);
   *   error = m_comm->send_response(m_event_ptr->addr, cbp);
   * </pre>
   *
   */
  class CommBuf {
  public:

    /**
     * This constructor initializes the CommBuf object by allocating a
     * primary buffer of length len and writing the header into it
     * with the given HeaderBuilder object, hbuilder.  It also sets the
     * extended buffer to _ex and takes ownership of it.  The total
     * length written into the header is len plus _exLen.  The internal
     * pointer into the primary buffer is positioned to just after the
     * header.
     *
     * @param hbuilder the Header builder object used to write the header
     * @param len the length of the primary buffer to allocate
     */
    CommBuf(HeaderBuilder &hbuilder, uint32_t len) : ext_ptr(0) {
      len += hbuilder.header_length();
      data.set(new uint8_t [len], len, true);
      data_ptr = data.base;
      hbuilder.set_total_len(len);
      hbuilder.encode(&data_ptr);
    }

    /**
     * This constructor initializes the CommBuf object by allocating a
     * primary buffer of length len and writing the header into it
     * with the given HeaderBuilder object, hbuilder.  It also sets the
     * extended buffer to _ex and takes ownership of it.  The total
     * length written into the header is len plus _exLen.  The internal
     * pointer into the primary buffer is positioned to just after the
     * header.
     *
     * @param hbuilder the Header builder object used to write the header
     * @param len the length of the primary buffer to allocate
     * @param buffer extended buffer
     */
    CommBuf(HeaderBuilder &hbuilder, uint32_t len, StaticBuffer &buffer) : ext(buffer) {
      len += hbuilder.header_length();
      data.set(new uint8_t [len], len, true);
      data_ptr = data.base;
      hbuilder.set_total_len(len+buffer.size);
      hbuilder.encode(&data_ptr);
      ext_ptr = ext.base;
    }

    /**
     * Resets the primary and extended data pointers to point to the
     * beginning of their respective buffers.  The AsyncComm layer
     * uses these pointers to track how much data has been sent and
     * what is remaining to be sent.
     */
    void reset_data_pointers() {
      HT_EXPECT((data_ptr - data.base) == (int)data.size, Error::FAILED_EXPECTATION);
      data_ptr = data.base;
      ext_ptr = ext.base;
    }

    /**
     * Returns the primary buffer internal data pointer
     */
    void *get_data_ptr() { return data_ptr; }

    /**
     * Returns address of the primary buffer internal data pointer
     */
    uint8_t **get_data_ptr_address() { return &data_ptr; }

    /**
     * Advance the primary buffer internal data pointer by len bytes
     *
     * @param len the number of bytes to advance the pointer by
     * @return returns the advanced internal data pointer
     */
    void *advance_data_ptr(size_t len) { data_ptr += len; return data_ptr; }

    /**
     * Append a boolean value to the primary buffer, advancing the
     * primary buffer internal data pointer by 1
     *
     * @param bval boolean value to append into buffer
     */
    void append_bool(bool bval) { Serialization::encode_bool(&data_ptr, bval); }

    /**
     * Append a byte of data to the primary buffer, advancing the
     * primary buffer internal data pointer by 1
     *
     * @param bval byte value to append into buffer
     */
    void append_byte(uint8_t bval) { *data_ptr++ = bval; }

    /**
     * Appends a sequence of bytes to the primary buffer, advancing
     * the primary buffer internal data pointer by the number of bytes appended
     *
     * @param bytes starting address of byte sequence
     * @param len number of bytes in sequence
     */
    void append_bytes(uint8_t *bytes, uint32_t len) {
      memcpy(data_ptr, bytes, len);
      data_ptr += len;
    }

    /**
     * Appends a c-style string to the primary buffer.  A string is encoded
     * as a 16-bit length, followed by the characters, followed by
     * a terminating '\\0'.
     *
     * @param str c-style string to append
     * @see Serialization::encode_str16
     */
    void append_str16(const char *str) {
      Serialization::encode_str16(&data_ptr, str);
    }

    /**
     * Appends a String to the primary buffer.  A string is encoded as
     * a 16-bit length, followed by the characters, followed by
     * a terminating '\\0'.
     *
     * @param str std string to append
     * @see Serialization::encode_str16
     */
    void append_str16(const String &str) {
      Serialization::encode_str16(&data_ptr, str);
    }

    /**
     * Appends a 16-bit integer to the primary buffer in little endian order,
     * advancing the primary buffer internal data pointer
     *
     * @param sval two byte short value to append into buffer
     */
    void append_i16(uint16_t sval) {
      Serialization::encode_i16(&data_ptr, sval);
    }

    /**
     * Appends a 32-bit integer to the primary buffer in little endian order,
     * advancing the primary buffer internal data pointer
     *
     * @param ival 4 byte integer value to append into buffer
     */
    void append_i32(uint32_t ival) {
      Serialization::encode_i32(&data_ptr, ival);
    }

    /**
     * Appends a 64-bit integer to the primary buffer in little endian order,
     * advancing the primary buffer internal data pointer
     *
     * @param lval 8 byte long integer value to append into buffer
     */
    void append_i64(uint64_t lval) {
      Serialization::encode_i64(&data_ptr, lval);
    }

    /**
     * Appends a c-style string to the primary buffer.  A string is encoded
     * as a vint64 length, followed by the characters, followed by
     * a terminating '\\0'.
     *
     * @param str c-style string to append
     * @see Serialization::encode_vstr
     */
    void append_vstr(const char *str) {
      Serialization::encode_vstr(&data_ptr, str);
    }

    /**
     * Appends a String to the primary buffer.  A string is encoded as
     * a vint64 length, followed by the characters, followed by
     * a terminating '\\0'.
     *
     * @param str std string to append
     * @see Serialization::encode_vstr
     */
    void append_vstr(const String &str) {
      Serialization::encode_vstr(&data_ptr, str);
    }

    /**
     * Appends a string buffer to the primary buffer.  A string is encoded
     * as a vint64 length, followed by the characters, followed by
     * a terminating '\\0'.
     *
     * @param buf input buffer pointer
     * @param len input len
     * @see Serialization::encode_vstr
     */
    void append_vstr(const void *str, uint32_t len) {
      Serialization::encode_vstr(&data_ptr, str, len);
    }

    friend class IOHandlerData;
    friend class IOHandlerDatagram;

    StaticBuffer data;
    StaticBuffer ext;

  protected:
    uint8_t *data_ptr;
    const uint8_t *ext_ptr;
  };

  typedef boost::shared_ptr<CommBuf> CommBufPtr;

}


#endif // HYPERTABLE_COMMBUF_H
