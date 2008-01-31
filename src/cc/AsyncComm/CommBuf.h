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

#ifndef HYPERTABLE_COMMBUF_H
#define HYPERTABLE_COMMBUF_H

#include <string>

#include <boost/shared_ptr.hpp>

extern "C" {
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
}

#include "Common/ByteString.h"
#include "Common/Logger.h"

#include "HeaderBuilder.h"
#include "Serialization.h"

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
   *   cbuf->append_short(COMMAND_STATUS);
   * </pre>
   *
   * The following is a real world example of a CommBuf being used
   * to send back a response from a read request.
   *
   * <pre>
   *   hbuilder.initialize_from_request(m_event_ptr->header);
   *   CommBufPtr cbufPtr( new CommBuf(hbuilder, 16, data, nread) );
   *   cbufPtr->append_int(Error::OK);
   *   cbufPtr->append_long(offset);
   *   cbufPtr->append_int(nread);
   *   error = m_comm->send_response(m_event_ptr->addr, cbufPtr);
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
     * @param _ex pointer to the extended buffer (this object takes ownership)
     * @param _exLen the length of the extended buffer
     */
    CommBuf(HeaderBuilder &hbuilder, uint32_t len, const void *_ex=0, uint32_t _exLen=0) {
      len += hbuilder.header_length();
      data = dataPtr = new uint8_t [ len ];
      dataLen = len;
      ext = extPtr = (const uint8_t *)_ex;
      extLen = _exLen;
      hbuilder.set_total_len(len+extLen);
      hbuilder.encode(&dataPtr);
    }

    /**
     * Destructor.  Deallocates primary and extended buffers.
     */
    ~CommBuf() {
      delete [] data;
      delete [] ext;
    }

    /**
     * Resets the primary and extended data pointers to point to the
     * beginning of their respective buffers.  The AsyncComm layer
     * uses these pointers to track how much data has been sent and
     * what is remaining to be sent.
     */
    void reset_data_pointers() {
      dataPtr = (uint8_t *)data;
      extPtr = ext;
    }

    /**
     * Returns the primary buffer internal data pointer
     */
    void *get_data_ptr() { return dataPtr; }

    /**
     * Returns address of the primary buffer internal data pointer
     */
    uint8_t **get_data_ptr_address() { return &dataPtr; }

    /**
     * Advance the primary buffer internal data pointer by len bytes
     *
     * @param len the number of bytes to advance the pointer by
     * @return returns the advanced internal data pointer
     */
    void *advance_data_ptr(size_t len) { dataPtr += len; return dataPtr; }

    /**
     * Append a boolean value to the primary buffer, advancing the
     * primary buffer internal data pointer by 1
     * 
     * @param bval boolean value to append into buffer
     */
    void append_bool(bool bval) { Serialization::encode_bool(&dataPtr, bval); }

    /**
     * Append a byte of data to the primary buffer, advancing the
     * primary buffer internal data pointer by 1
     * 
     * @param bval byte value to append into buffer
     */
    void append_byte(uint8_t bval) { *dataPtr++ = bval; }

    /**
     * Appends a sequence of bytes to the primary buffer, advancing
     * the primary buffer internal data pointer by the number of bytes appended
     * 
     * @param bytes starting address of byte sequence
     * @param len number of bytes in sequence
     */
    void append_bytes(uint8_t *bytes, uint32_t len) { memcpy(dataPtr, bytes, len); dataPtr += len; }

    /**
     * Appends a short integer (16 bit) to the the primary buffer,
     * advancing the primary buffer internal data pointer
     *
     * @param sval two byte short value to append into buffer
     */
    void append_short(uint16_t sval) { Serialization::encode_short(&dataPtr, sval); }
    
    /**
     * Appends an integer (32 bit) to the the primary buffer,
     * advancing the primary buffer internal data pointer
     *
     * @param ival 4 byte integer value to append into buffer
     */
    void append_int(uint32_t ival) { Serialization::encode_int(&dataPtr, ival); }

    /**
     * Appends a long integer (64 bit) to the the primary buffer,
     * advancing the primary buffer internal data pointer
     *
     * @param lval 8 byte long integer value to append into buffer
     */
    void append_long(uint64_t lval) { Serialization::encode_long(&dataPtr, lval); }

    /**
     * Appends a byte array to the primary buffer.  A byte array
     * is encoded as a length followd by the data.
     *
     * @param data starting address of byte array to append
     * @param len length of byte array to append
     * @see Serialization::encode_byte_array
     */
    void append_byte_array(const void *data, int32_t len) { Serialization::encode_byte_array(&dataPtr, data, len); }
    
    /**
     * Appends a c-style string to the primary buffer.  A string is
     * encoded as a length, followed by the characters, followed by
     * a terminating '\\0'.
     *
     * @param str c-style string to append
     * @see Serialization::encode_string
     */
    void append_string(const char *str) { Serialization::encode_string(&dataPtr, str); }

    /**
     * Appends a std::string to the primary buffer.  A string is
     * encoded as a length, followed by the characters, followed by
     * a terminating '\\0'.
     *
     * @param str std string to append
     * @see Serialization::encode_string
     */
    void append_string(const std::string &str) { Serialization::encode_string(&dataPtr, str.c_str()); }

    friend class IOHandlerData;

    const uint8_t *data;
    uint32_t dataLen;
    const uint8_t *ext;
    uint32_t extLen;

  protected:
    uint8_t *dataPtr;
    const uint8_t *extPtr;
  };

  typedef boost::shared_ptr<CommBuf> CommBufPtr;
  
}


#endif // HYPERTABLE_COMMBUF_H
