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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_SERIALIZATION_H
#define HYPERTABLE_SERIALIZATION_H

#include "InetAddr.h"

#include "serialization-c.h"


namespace Hypertable { namespace Serialization {

  /**
   * Encodes a byte into the given buffer.  Assumes there is
   * enough space available.  Increments buffer pointer.
   *
   * @param bufp address of destination buffer
   * @param val the byte
   */
  inline void encode_i8(uint8_t **bufp, uint8_t val) {
    HT_ENCODE_I8(*bufp, val);
  }

  /**
   * Decode a 8-bit integer (a byte/character)
   *
   * @param bufp - pointer to the source buffer
   * @param remainp - pointer to the remaining size variable
   * @return value
   */
  inline uint8_t decode_i8(const uint8_t **bufp, size_t *remainp) {
    HT_DECODE_NEED(*remainp, 1);
    return *(*bufp)++;
  }

  /**
   * Decodes a single byte from the given buffer. Increments buffer pointer
   * and decrements remainp on success.
   *
   * @param bufp pointer of source buffer
   * @param remainp pointer to variable of number of bytes remaining in buffer
   * @return value of byte
   */
  inline uint8_t decode_byte(const uint8_t **bufp, size_t *remainp) {
    return decode_i8(bufp, remainp);
  }

  /**
   * Encodes a boolean into the given buffer.  Assumes there is
   * enough space available.  Increments buffer pointer.
   *
   * @param bufp address of destination buffer
   * @param bval the boolean value
   */
  inline void encode_bool(uint8_t **bufp, bool bval) {
    HT_ENCODE_BOOL(*bufp, bval);
  }

  /**
   * Decodes a boolean value from the given buffer. Increments buffer pointer
   * and decrements remainp on success.
   *
   * @param bufp pointer to source buffer
   * @param remainp pointer to variable of number of bytes remaining in buffer
   * @return boolean value
   */
  inline bool decode_bool(const uint8_t **bufp, size_t *remainp) {
    return decode_i8(bufp, remainp) != 0;
  }

  /**
   * Encode a 16-bit integer in little-endian order
   *
   * @param bufp - pointer to the destination buffer
   * @param val value to encode
   */
  inline void encode_i16(uint8_t **bufp , uint16_t val) {
    HT_ENCODE_I16(*bufp, val);
  }

  /**
   * Decode a 16-bit integer in little-endian order
   *
   * @param bufp - pointer to the source buffer
   * @param remainp - pointer to remaining size variable
   * @return value
   */
  inline uint16_t decode_i16(const uint8_t **bufp, size_t *remainp) {
    uint16_t val;
    HT_DECODE_I16(*bufp, *remainp, val);
    return val;
  }

  /**
   * Encode a 32-bit integer in little-endian order
   *
   * @param bufp - pointer to the destination buffer
   * @param val - value to encode
   */
  inline void encode_i32(uint8_t **bufp, uint32_t val) {
    HT_ENCODE_I32(*bufp, val);
  }

  /**
   * Decode a 32-bit integer in little-endian order
   *
   * @param bufp - pointer to the source buffer
   * @param remainp - pointer to remaining size variable
   * @return value
   */
  inline uint32_t decode_i32(const uint8_t **bufp, size_t *remainp) {
    uint32_t val;
    HT_DECODE_I32(*bufp, *remainp, val);
    return val;
  }

  /**
   * Encode a 64-bit integer in little-endian order
   *
   * @param bufp - pointer to the destination buffer
   * @param val - value to encode
   */
  inline void encode_i64(uint8_t **bufp, uint64_t val) {
    HT_ENCODE_I64(*bufp, val);
  }

  /**
   * Decode a 64-bit integer in little-endian order
   *
   * @param bufp - pointer to the source buffer
   * @param remainp - pointer to remaining size variable
   * @return value
   */
  inline uint64_t decode_i64(const uint8_t **bufp, size_t *remainp) {
    uint64_t val;
    HT_DECODE_I64(*bufp, *remainp, val);
    return val;
  }

  /**
   * Length of a variable length encoded 32-bit integer (up to 5 bytes)
   *
   * @param val - 32-bit integer to encode
   * @return number of bytes required
   */
  inline int encoded_length_vi32(uint32_t val) {
    return HT_ENCODED_LEN_VI32(val);
  }

  /**
   * Length of a variable length encoded 64-bit integer (up to 9 bytes)
   *
   * @param val - 64-bit integer to encode
   * @return number of bytes required
   */
  inline int encoded_length_vi64(uint64_t val) {
    return HT_ENCODED_LEN_VI64(val);
  }

  /**
   * Encode a integer (up to 32-bit) in variable length encoding
   *
   * @param bufp - pointer to the destination buffer
   * @param val - value to encode
   */
  inline void encode_vi32(uint8_t **bufp, uint32_t val) {
    HT_ENCODE_VI32(*bufp, val, return);
  }

  /**
   * Encode a integer (up to 64-bit) in variable length encoding
   *
   * @param bufp - pointer to the destination buffer
   * @param val - value to encode
   */
  inline void encode_vi64(uint8_t **bufp, uint64_t val) {
    HT_ENCODE_VI64(*bufp, val, return);
  }

  /**
   * Decode a variable length encoded integer up to 32-bit
   *
   * @param bufp - pointer to the source buffer
   * @param remainp - pointer to remaining size variable
   * @return value
   */
  inline uint32_t decode_vi32(const uint8_t **bufp, size_t *remainp) {
    uint32_t n;
    HT_DECODE_VI32(*bufp, *remainp, n, return n);
  }

  /**
   * Decode a variable length encoded integer up to 64-bit
   *
   * @param bufp - pointer to the source buffer
   * @param remainp - pointer to remaining size variable
   * @return value
   */
  inline uint64_t decode_vi64(const uint8_t **bufp, size_t *remainp) {
    uint64_t n;
    HT_DECODE_VI64(*bufp, *remainp, n, return n);
  }

  /**
   * Decode a variable length encoded integer up to 32-bit
   *
   * @param bufp - pointer to the source buffer
   * @return value
   */
  inline uint32_t decode_vi32(const uint8_t **bufp) {
    size_t remain = 6;
    return decode_vi32(bufp, &remain);
  }

  /**
   * Decode a variable length encoded integer up to 64-bit
   *
   * @param bufp - pointer to the source buffer
   * @return value
   */
  inline uint64_t decode_vi64(const uint8_t **bufp) {
    size_t remain = 12;
    return decode_vi64(bufp, &remain);
  }

  /**
   * Computes the encoded length of a 32-bit length byte array (i32, bytes)
   *
   * @param len length of the byte array to be encoded
   * @return the encoded length of a byte array of length len
   */
  inline size_t encoded_length_bytes32(int32_t len) {
    return len + 4;
  }

  /**
   * Encodes a variable sized byte array into the given buffer.  Encoded as a
   * 4 byte length followed by the data.  Assumes there is enough space
   * available.  Increments buffer pointer.
   *
   * @param bufp address of destinatin buffer
   * @param data pointer to array of bytes
   * @param len the length of the byte array
   */
  inline void encode_bytes32(uint8_t **bufp, const void *data, int32_t len) {
    HT_ENCODE_BYTES32(*bufp, data, len);
  }

  /**
   * Decodes a variable sized byte array from the given buffer.  Byte array i
   * encoded as a 4 byte length followed by the data.  Increments buffer
   * pointer and decrements remainp on success.
   *
   * @param bufp address of buffer containing encoded byte array
   * @param remainp address of variable containing number of bytes remaini
   * @param lenp address of length of decoded byte array
   */
  inline uint8_t *decode_bytes32(const uint8_t **bufp, size_t *remainp,
                                 uint32_t *lenp) {
    uint8_t *out;
    HT_DECODE_BYTES32(*bufp, *remainp, out, *lenp);
    return out;
  }

  /**
   * Computes the encoded length of a string16 encoding
   *
   * @param str pointer to the the c-style string
   * @return the encoded length of str
   */
  inline size_t encoded_length_str16(const char *str) {
    return 2 + ((str == 0) ? 0 : strlen(str)) + 1;
  }

  /**
   * Computes the encoded length of a String.
   *
   * @param str reference to string object
   * @return the encoded length of str
   */
  inline size_t encoded_length_str16(const String &str) {
    return 2 + str.length() + 1;
  }

  /**
   * Encodes a string buffer into the given buffer.
   * Encoded as a 2 byte length followed by the string data, followed
   * by a '\\0' termination byte.  The length value does not include
   * the '\\0'.  Assumes there is enough space available.  Increments
   * buffer pointer.
   *
   * @param bufp address of destination buffer
   * @param str the c-style string to encode
   * @param len length of the string
   */
  inline void encode_str16(uint8_t **bufp, const void *str, uint16_t len) {
    HT_ENCODE_STR16(*bufp, str, len);
  }

  /**
   * Encodes a c-style null-terminated string into the given buffer.
   * Encoded as a 2 byte length followed by the string data, followed
   * by a '\\0' termination byte.  The length value does not include
   * the '\\0'.  Assumes there is enough space available.  Increments
   * buffer pointer.
   *
   * @param bufp address of destination buffer
   * @param str the c-style string to encode
   */
  inline void encode_str16(uint8_t **bufp, const char *str) {
    uint16_t len = (str == 0) ? 0 : strlen(str);
    encode_str16(bufp, str, len);
  }

  /**
   * Encodes a string into the given buffer.  Encoded as a
   * 2 byte length followed by the string data, followed by a '\\0'
   * termination byte.  The length value does not include the '\\0'.
   * Assumes there is enough space available.  Increments buffer
   * pointer.
   *
   * @param bufp address of destinatin buffer
   * @param str the String to encode
   */
  template <class StringT>
  inline void encode_str16(uint8_t **bufp, const StringT &str) {
    encode_str16(bufp, str.c_str(), str.length());
  }

  /**
   * Decodes a c-style string from the given buffer.  The encoding of the
   * string is a 2 byte length followed by the string, followed by a '\\0'
   * termination byte.  The length does not include the '\\0' terminator.
   * The decoded string pointer points back into the encoding buffer.
   * Increments buffer pointer and decrements remainp on success.
   *
   * @param bufp address of buffer containing encoded string
   * @param remainp address of variable of number of bytes remaining in buffer
   * @return pointer to a c-style string
   */
  inline const char *decode_str16(const uint8_t **bufp, size_t *remainp) {
    const char *str;
    uint16_t len;
    HT_DECODE_STR16(*bufp, *remainp, str, len);
    return str;
  }

  /**
   * Decodes a c-style string from the given buffer.  The encoding of the
   * string is a 2 byte length followed by the string, followed by a '\\0'
   * termination byte.  The length does not include the '\\0' terminator.
   * The decoded string pointer points back into the encoding buffer.
   * Increments buffer pointer and decrements remainp on success.
   *
   * @param bufp address of buffer containing encoded string
   * @param remainp address of variable of number of bytes remaining in buffer
   * @param lenp address of varaible to hold the len of the string
   * @return pointer to a c-style string
   */
  inline char *decode_str16(const uint8_t **bufp, size_t *remainp,
                                  uint16_t *lenp) {
    char *str;
    HT_DECODE_STR16(*bufp, *remainp, str, *lenp);
    return str;
  }

  /**
   * Decodes a str16 from the given buffer to a String.  The encoding of the
   * string is a 2 byte length followed by the string, followed by a '\\0'
   * termination byte.  The length does not include the '\\0' terminator.
   * Increments buffer pointer and decrements remainp on success.
   *
   * @param bufp address of buffer containing encoded string
   * @param remainp address of variable of number of bytes remaining in buffer
   * @return true on success, false if buffer has insufficient room
   */
  template <class StringT>
  inline StringT decode_str16(const uint8_t **bufp, size_t *remainp) {
    char *str;
    uint16_t len;
    HT_DECODE_STR16(*bufp, *remainp, str, len);
    return StringT(str, len); // RVO hopeful
  }

  /**
   * Computes the encoded length of vstr (vint64, data, null)
   *
   * @param len string length
   * @return the encoded length of str
   */
  inline size_t encoded_length_vstr(size_t len) {
    return encoded_length_vi64(len) + len + 1;
  }

  /**
   * Computes the encoded length of vstr
   * Assuming string length can be encoded in 32-bit integer
   *
   * @param s pointer to the the c-style string
   * @return the encoded length of str
   */
  inline size_t encoded_length_vstr(const char *s) {
    return encoded_length_vstr(s ? strlen(s) : 0);
  }

  /**
   * Computes the encoded length of vstr
   * Assuming string length can be encoded in 32-bit integer
   *
   * @param s string to encode
   * @return the encoded length of str
   */
  inline size_t encoded_length_vstr(const String &s) {
    return encoded_length_vstr(s.length());
  }

  /**
   * Encode a buffer as vstr (vint64, data, null)
   *
   * @param bufp - pointer to destination buffer
   * @param buf - pointer to the start of the input buffer
   * @param len - length of the input buffer
   */
  inline void encode_vstr(uint8_t **bufp, const void *buf, size_t len) {
    HT_ENCODE_VSTR(*bufp, buf, len);
  }

  /**
   * Encode a c-style string as vstr
   *
   * @param bufp - pointer to destination buffer
   * @param s - pointer to the start of the string
   */
  inline void encode_vstr(uint8_t **bufp, const char *s) {
    encode_vstr(bufp, s, s ? strlen(s) : 0);
  }

  /**
   * Encode a String as vstr
   *
   * @param bufp - pointer to destination buffer
   * @param s - string to encode
   */
  template <class StringT>
  inline void encode_vstr(uint8_t **bufp, const StringT &s) {
    encode_vstr(bufp, s.data(), s.length());
  }

  /**
   * Decode a vstr (vint64, data, null)
   * Note, decoding a vstr longer than 4GiB on a 32-bit platform
   * is obviously futile (throws bad vstr exception)
   *
   * @param bufp - pointer to the source buffer
   * @param remainp - pointer to the remaining size variable
   * @return str
   */
  inline char *decode_vstr(const uint8_t **bufp, size_t *remainp) {
    char *buf;
    size_t len;
    HT_DECODE_VSTR(*bufp, *remainp, buf, len);
    return buf;
  }

  /**
   * Decode a vstr (vint64, data, null)
   *
   * @param bufp - pointer to the source buffer
   * @param remainp - pointer to the remaining size variable
   * @return str
   */
  template <class StringT>
  inline String decode_vstr(const uint8_t **bufp, size_t *remainp) {
    char *buf;
    size_t len;
    HT_DECODE_VSTR(*bufp, *remainp, buf, len);
    return StringT(buf, len); // RVO
  }

  /**
   * Decode a vstr (vint64, data, null)
   *
   * @param bufp - pointer to the source buffer
   * @param remainp - pointer to the remaining size variable
   * @param lenp - pointer to
   * @return str
   */
  inline char *decode_vstr(const uint8_t **bufp, size_t *remainp,
                           uint32_t *lenp) {
    char *buf;
    HT_DECODE_VSTR(*bufp, *remainp, buf, *lenp);
    return buf;
  }


  /**
   * Encode an InetAddr structure
   *
   * @param bufp - pointer to the destination buffer
   * @param addr - address to encode
   */
  inline void encode_inet_addr(uint8_t **bufp, const InetAddr &addr) {
    HT_ENCODE_I16(*bufp, addr.sin_family);
    HT_ENCODE_I32(*bufp, addr.sin_addr.s_addr);
    HT_ENCODE_I16(*bufp, addr.sin_port);
  }

  /**
   * Decode an InetAddr structure
   *
   * @param bufp - pointer to the source buffer
   * @param remainp - pointer to remaining size variable
   * @return value
   */
  inline InetAddr decode_inet_addr(const uint8_t **bufp, size_t *remainp) {
    InetAddr addr;
    HT_DECODE_I16(*bufp, *remainp, addr.sin_family);
    HT_DECODE_I32(*bufp, *remainp, addr.sin_addr.s_addr);
    HT_DECODE_I16(*bufp, *remainp, addr.sin_port);
    return addr;
  }


}} // namespace Hypertable::Serialization

#endif // HYPERTABLE_SERIALIZATION_H
