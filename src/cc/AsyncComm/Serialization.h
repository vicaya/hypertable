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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_SERIALIZATION_H
#define HYPERTABLE_SERIALIZATION_H

#include <cstring>
#include <string>

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/ByteString.h"

// vint limits
#define HT_MAX_V1B 0x7f
#define HT_MAX_V2B 0x3fff
#define HT_MAX_V3B 0x1fffff
#define HT_MAX_V4B 0xfffffff
#define HT_MAX_V5B 0x7ffffffffull
#define HT_MAX_V6B 0x3ffffffffffull
#define HT_MAX_V7B 0x1ffffffffffffull
#define HT_MAX_V8B 0xffffffffffffffull
#define HT_MAX_V9B 0x7fffffffffffffffull

// vint encode
#define HT_ENCODE_VINT0(_op_, _val_) \
  if (_val_ <= HT_MAX_V1B) { \
    *(_op_)++ = (uint8_t)(_val_ & 0x7f); \
    return; \
  }

#define HT_ENCODE_VINT_(_op_, _val_) \
  *(_op_)++ = (uint8_t)(_val_ | 0x80); \
  _val_ >>= 7; \
  HT_ENCODE_VINT0(_op_, _val_)

#define HT_ENCODE_VINT4(_op_, _val_) \
  HT_ENCODE_VINT_(_op_, _val_) HT_ENCODE_VINT_(_op_, _val_) \
  HT_ENCODE_VINT_(_op_, _val_) HT_ENCODE_VINT_(_op_, _val_)

// vint decode
#define HT_DECODE_NEED(_rp_, _n_) \
  if (*(_rp_) < _n_) \
    throw Exception(Error::SERIALIZATION_INPUT_OVERRUN); \
  *(_rp_) -= _n_

#define HT_DECODE_VINT0(_type_, _n_, _ip_, _rp_) \
  HT_DECODE_NEED(_rp_, 1); \
  _type_ _n_ = (*(_ip_)++ & 0x7f), shift = 0;

#define HT_DECODE_VINT_(_type_, _n_, _ip_, _rp_) \
  shift += 7; \
  if ((_ip_)[-1] & 0x80) { \
    HT_DECODE_NEED(_rp_, 1); \
    _n_ |= ((_type_)(*(_ip_)++ & 0x7f) << shift); \
  } else return _n_;

#define HT_DECODE_VINT4(_type_, _n_, _ip_, _rp_) \
  HT_DECODE_VINT_(_type_, _n_, _ip_, _rp_) \
  HT_DECODE_VINT_(_type_, _n_, _ip_, _rp_) \
  HT_DECODE_VINT_(_type_, _n_, _ip_, _rp_) \
  HT_DECODE_VINT_(_type_, _n_, _ip_, _rp_)

namespace Hypertable { namespace Serialization {

    /**
     * Encodes a boolean into the given buffer.  Assumes there is
     * enough space available.  Increments buffer pointer.
     *
     * @param bufPtr address of destination buffer
     * @param bval the boolean value
     */
    inline void encode_bool(uint8_t **bufPtr, bool bval) {
      *(*bufPtr) = (bval) ? 1 : 0;
      (*bufPtr)++;
    }


    /**
     * Decodes a boolean value from the given buffer.  Increments buffer pointer
     * and decrements remainingPtr on success.
     *
     * @param bufPtr address of source buffer
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @param bytePtr address of variable to hold decoded byte
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_bool(uint8_t **bufPtr, size_t *remainingPtr, bool *boolp) {
      if (*remainingPtr == 0)
	return false;
      *boolp = (*(*bufPtr) == 0) ? false : true;
      (*bufPtr)++;
      (*remainingPtr)--;
      return true;
    }


    /**
     * Decodes a single byte from the given buffer.  Increments buffer pointer
     * and decrements remainingPtr on success.
     *
     * @param bufPtr address of source buffer
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @param bytePtr address of variable to hold decoded byte
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_byte(uint8_t **bufPtr, size_t *remainingPtr, uint8_t *bytePtr) {
      if (*remainingPtr == 0)
	return false;
      *bytePtr = *(*bufPtr)++;
      (*remainingPtr)--;
      return true;
    }


    /**
     * Encodes a short (2 byte integer) into the given buffer.  Assumes there is
     * enough space available.  Increments buffer pointer.
     *
     * @param bufPtr address of destinatin buffer
     * @param sval the short value to encode
     */
    inline void encode_short(uint8_t **bufPtr, uint16_t sval) {
      memcpy(*bufPtr, &sval, 2);
      (*bufPtr) += 2;
    }


    /**
     * Decodes a short (2 byte integer) from the given buffer.  Increments
     * buffer pointer and decrements remainingPtr on success.
     *
     * @param bufPtr address of buffer containing encoded short
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @param shortPtr address of variable to hold decoded short
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_short(uint8_t **bufPtr, size_t *remainingPtr, uint16_t *shortPtr) {
      if (*remainingPtr < 2)
	return false;
      memcpy(shortPtr, *bufPtr, 2);
      (*remainingPtr) -= 2;
      (*bufPtr) += 2;
      return true;
    }


    /**
     * Encodes an int (4 byte integer) into the given buffer.  Assumes there is
     * enough space available.  Increments buffer pointer.
     *
     * @param bufPtr address of destinatin buffer
     * @param ival the int value to encode
     */
    inline void encode_int(uint8_t **bufPtr, uint32_t ival) {
      memcpy(*bufPtr, &ival, 4);
      *bufPtr += 4;
    }


    /**
     * Decodes an int (4 byte integer) from the given buffer.  Increments
     * buffer pointer and decrements remainingPtr on success.
     *
     * @param bufPtr address of buffer containing encoded int
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @param intPtr address of variable to hold decoded int
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_int(uint8_t **bufPtr, size_t *remainingPtr, uint32_t *intPtr) {
      if (*remainingPtr < 4)
	return false;
      memcpy(intPtr, *bufPtr, 4);
      (*remainingPtr) -= 4;
      (*bufPtr) += 4;
      return true;
    }


    /**
     * Encodes a long (8 byte integer) into the given buffer.  Assumes there is
     * enough space available.  Increments buffer pointer.
     *
     * @param bufPtr address of destinatin buffer
     * @param lval the long value to encode
     */
    inline void encode_long(uint8_t **bufPtr, uint64_t lval) {
      memcpy(*bufPtr, &lval, 8);
      *bufPtr += 8;
    }


    /**
     * Decodes an int (4 byte integer) from the given buffer.  Increments
     * buffer pointer and decrements remainingPtr on success.
     *
     * @param bufPtr address of buffer containing encoded long
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @param longPtr address of variable to hold decoded long
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_long(uint8_t **bufPtr, size_t *remainingPtr, uint64_t *longPtr) {
      if (*remainingPtr < 8)
	return false;
      memcpy(longPtr, *bufPtr, 8);
      (*remainingPtr) -= 8;
      (*bufPtr) += 8;
      return true;
    }

    /**
     * Computes the encoded length of a byte array
     *
     * @param len length of the byte array to be encoded
     * @return the encoded length of a byte array of length len
     */
    inline size_t encoded_length_byte_array(int32_t len) {
      return len + 4;
    }


    /**
     * Encodes a variable sized byte array into the given buffer.  Encoded as a
     * 4 byte length followed by the data.  Assumes there is enough space
     * available.  Increments buffer pointer.
     *
     * @param bufPtr address of destinatin buffer
     * @param data pointer to array of bytes
     * @param len the length of the byte array
     */
    inline void encode_byte_array(uint8_t **bufPtr, const void *data, int32_t len) {
      memcpy(*bufPtr, &len, 4);
      memcpy((*bufPtr)+4, (uint8_t *)data, len);
      *bufPtr += len + 4;
    }


    /**
     * Decodes a variable sized byte array from the given buffer.  Byte array is
     * encoded as a 4 byte length followed by the data.  Increments buffer
     * pointer and decrements remainingPtr on success.
     *
     * @param bufPtr address of buffer containing encoded byte array
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @param dstPtr address of pointer to decoded byte array
     * @param lenPtr address of length of decoded byte array
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_byte_array(uint8_t **bufPtr, size_t *remainingPtr, uint8_t **dstPtr, int32_t *lenPtr) {
      // length
      if (*remainingPtr < 4)
	return false;
      memcpy(lenPtr, *bufPtr, 4);
      (*remainingPtr) -= 4;
      (*bufPtr) += 4;
      // data
      if (*remainingPtr < (size_t)*lenPtr)
	return false;
      *dstPtr = (*bufPtr);
      (*remainingPtr) -= *lenPtr;
      (*bufPtr) += *lenPtr;
      return true;
    }

    /**
     * Decodes a ByteString32T from the given buffer.  The encoding of the
     * ByteString32T is the same as a byte array, a 4 byte length followed
     * by the data.  The decoded byte string pointer bs32Ptr points back
     * into the encoding buffer.  Increments buffer pointer and decrements
     * remainingPtr on success.
     *
     * @param bufPtr address of buffer containing encoded ByteString32T
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @param bs32Ptr address of ByteString32T pointer which points into buffer on success
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_byte_string32(const uint8_t **bufPtr, size_t *remainingPtr, ByteString32T **bs32Ptr) {
      if (*remainingPtr < 4)
	return false;
      *bs32Ptr = (ByteString32T *)*bufPtr;
      if (*remainingPtr < (*bs32Ptr)->len + 4)
	return false;
      (*remainingPtr) -= Length(*bs32Ptr);
      (*bufPtr) += Length(*bs32Ptr);
      return true;
    }


    /**
     * Computes the encoded length of a c-style null-terminated string
     *
     * @param str pointer to the the c-style string
     * @return the encoded length of str
     */
    inline size_t encoded_length_string(const char *str) {
      return 2 + ((str == 0) ? 0 : strlen(str)) + 1;
    }


    /**
     * Computes the encoded length of a std::string.
     *
     * @param str reference to string object
     * @return the encoded length of str
     */
    inline size_t encoded_length_string(const std::string &str) {
      return encoded_length_string(str.c_str());
    }


    /**
     * Encodes a c-style null-terminated string into the given buffer.
     * Encoded as a 2 byte length followed by the string data, followed
     * by a '\\0' termination byte.  The length value does not include
     * the '\\0'.  Assumes there is enough space available.  Increments
     * buffer pointer.
     *
     * @param bufPtr address of destination buffer
     * @param str the c-style string to encode
     */
    inline void encode_string(uint8_t **bufPtr, const char *str) {
      uint16_t len = (str == 0) ? 0 : strlen(str);

      // 2-byte length
      memcpy(*bufPtr, &len, 2);
      (*bufPtr) += 2;

      // string characters
      if (len > 0) {
	memcpy(*bufPtr, str, len);
	(*bufPtr) += len;
      }

      // '\\0' terminator
      *(*bufPtr)++ = 0;
    }


    /**
     * Encodes a std::string into the given buffer.  Encoded as a
     * 2 byte length followed by the string data, followed by a '\\0'
     * termination byte.  The length value does not include the '\\0'.
     * Assumes there is enough space available.  Increments buffer
     * pointer.
     *
     * @param bufPtr address of destinatin buffer
     * @param str the std::string to encode
     */
    inline void encode_string(uint8_t **bufPtr, const std::string &str) {
      encode_string(bufPtr, str.c_str());
    }


    /**
     * Decodes a c-style string from the given buffer.  The encoding of the
     * string is a 2 byte length followed by the string, followed by a '\\0'
     * termination byte.  The length does not include the '\\0' terminator.
     * The decoded string pointer points back into the encoding buffer.
     * Increments buffer pointer and decrements remainingPtr on success.
     *
     * @param bufPtr address of buffer containing encoded string
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @param strPtr address of string pointer, points back into encoding buffer on success
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_string(uint8_t **bufPtr, size_t *remainingPtr, const char **strPtr) {
      uint16_t len;
      *strPtr = 0;
      if (*remainingPtr < 3)
	return false;
      memcpy(&len, *bufPtr, 2);
      if (*remainingPtr < (size_t)len+3)
	return false;
      *strPtr = (const char *)((*bufPtr)+2);
      (*remainingPtr) -= (size_t)len+3;
      (*bufPtr) += (size_t)len+3;
      return true;
    }


    /**
     * Decodes a std::string from the given buffer.  The encoding of the
     * string is a 2 byte length followed by the string, followed by a '\\0'
     * termination byte.  The length does not include the '\\0' terminator.
     * Increments buffer pointer and decrements remainingPtr on success.
     *
     * @param bufPtr address of buffer containing encoded string
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @param str reference to std::string to hold result
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_string(uint8_t **bufPtr, size_t *remainingPtr, std::string &str) {
      const char *cstr;
      if (!decode_string(bufPtr, remainingPtr, &cstr))
	return false;
      str = cstr;
      return true;
    }

    /**
     * Decode a 8-bit integer (a byte/character)
     *
     * @param bufp - pointer to the source buffer
     * @param remainp - pointer to the remaining size variable
     * @return value
     */
    inline uint8_t decode_i8(const uint8_t **bufp, size_t *remainp) {
      if (*remainp < 1)
        throw Exception(Error::SERIALIZATION_INPUT_OVERRUN);

      *remainp -= 1;
      return *(*bufp)++;
    }


    /**
     * Encode a 16-bit integer in network byte order
     *
     * @param bufp - pointer to the destination buffer
     * @param val value to encode
     */
    inline void encode_i16(uint8_t **bufp , uint16_t val) {
      *(*bufp)++ = (uint8_t)(val >> 8);
      *(*bufp)++ = (uint8_t)val;
    }


    /**
     * Decode a 16-bit integer in network byte order
     *
     * @param bufp - pointer to the source buffer
     * @param remainp - pointer to remaining size variable
     * @return value
     */
    inline uint16_t decode_i16(const uint8_t **bufp, size_t *remainp) {
      if (*remainp < 2)
        throw Exception(Error::SERIALIZATION_INPUT_OVERRUN);

      uint16_t val = (*(*bufp)++ << 8);
      val |= *(*bufp)++;
      *remainp -= 2;

      return val;
    }


    /**
     * Encode a 32-bit integer in network byte order
     *
     * @param bufp - pointer to the destination buffer
     * @param val - value to encode
     */
    inline void encode_i32(uint8_t **bufp, uint32_t val) {
      *(*bufp)++ = (uint8_t)(val >> 24);
      *(*bufp)++ = (uint8_t)(val >> 16);
      *(*bufp)++ = (uint8_t)(val >> 8);
      *(*bufp)++ = (uint8_t)val;
    }


    /**
     * Decode a 32-bit integer in network byte order
     *
     * @param bufp - pointer to the source buffer
     * @param remainp - pointer to remaining size variable
     * @return value
     */
    inline uint32_t decode_i32(const uint8_t **bufp, size_t *remainp) {
      if (*remainp < 4)
        throw Exception(Error::SERIALIZATION_INPUT_OVERRUN);

      uint32_t val = (*(*bufp)++ << 24);
      val |= (*(*bufp)++ << 16);
      val |= (*(*bufp)++ << 8);
      val |= *(*bufp)++;
      *remainp -= 4;

      return val;
    }


    /**
     * Encode a 64-bit integer in network byte order
     *
     * @param bufp - pointer to the destination buffer
     * @param val - value to encode
     */
    inline void encode_i64(uint8_t **bufp, uint64_t val) {
      *(*bufp)++ = (uint8_t)(val >> 56);
      *(*bufp)++ = (uint8_t)(val >> 48);
      *(*bufp)++ = (uint8_t)(val >> 40);
      *(*bufp)++ = (uint8_t)(val >> 32);
      *(*bufp)++ = (uint8_t)(val >> 24);
      *(*bufp)++ = (uint8_t)(val >> 16);
      *(*bufp)++ = (uint8_t)(val >> 8);
      *(*bufp)++ = (uint8_t)val;
    }


    /**
     * Decode a 64-bit integer in network byte order
     *
     * @param bufp - pointer to the source buffer
     * @param remainp - pointer to remaining size variable
     * @return value
     */
    inline uint64_t decode_i64(const uint8_t **bufp, size_t *remainp) {
      if (*remainp < 8)
        throw Exception(Error::SERIALIZATION_INPUT_OVERRUN);

      uint64_t val = ((uint64_t)*(*bufp)++ << 56);
      val |= ((uint64_t)(*(*bufp)++) << 48);
      val |= ((uint64_t)(*(*bufp)++) << 40);
      val |= ((uint64_t)(*(*bufp)++) << 32);
      val |= ((uint64_t)(*(*bufp)++) << 24);
      val |= (*(*bufp)++ << 16);
      val |= (*(*bufp)++ << 8);
      val |= *(*bufp)++;
      *remainp -= 8;

      return val;
    }


    /**
     * Length of a variable length encoded 32-bit integer (up to 5 bytes)
     *
     * @param val - 32-bit integer to encode
     * @return number of bytes required
     */
    inline int encoded_length_vi32(uint32_t val) {
       return (val > HT_MAX_V4B ? 5 : \
               (val > HT_MAX_V3B ? 4 : \
                (val > HT_MAX_V2B ? 3 : \
                 (val > HT_MAX_V1B ? 2 : 1))));
    }

    /**
     * Length of a variable length encoded 64-bit integer (up to 9 bytes)
     *
     * @param val - 64-bit integer to encode
     * @return number of bytes required
     */
    inline int encoded_length_vi64(uint64_t val) {
      return (val > HT_MAX_V9B ? 10 : \
              (val > HT_MAX_V8B ? 9 : \
               (val > HT_MAX_V7B ? 8 : \
                (val > HT_MAX_V6B ? 7 : \
                 (val > HT_MAX_V5B ? 6 : \
                  (val > HT_MAX_V4B ? 5 : \
                   (val > HT_MAX_V3B ? 4 : \
                    (val > HT_MAX_V2B ? 3 : \
                     (val > HT_MAX_V1B ? 2 : 1)))))))));
    }

    /**
     * Encode a integer (up to 32-bit) in variable length encoding
     *
     * @param bufp - pointer to the destination buffer
     * @param val - value to encode
     */
    inline void encode_vi32(uint8_t **bufp, uint32_t val) {
      HT_ENCODE_VINT0(*bufp, val)
      HT_ENCODE_VINT4(*bufp, val)
      HT_ENCODE_VINT_(*bufp, val)
      HT_EXPECT(!"bad/corrupted code", Error::FAILED_EXPECTATION);
    }

    /**
     * Encode a integer (up to 64-bit) in variable length encoding
     *
     * @param bufp - pointer to the destination buffer
     * @param val - value to encode
     */
    inline void encode_vi64(uint8_t **bufp, uint64_t val) {
      HT_ENCODE_VINT0(*bufp, val)
      HT_ENCODE_VINT4(*bufp, val)
      HT_ENCODE_VINT4(*bufp, val)
      HT_ENCODE_VINT_(*bufp, val)
      HT_EXPECT(!"bad/corrupted code", Error::FAILED_EXPECTATION);
    }

    /**
     * Decode a variable length encoded integer up to 32-bit
     *
     * @param bufp - pointer to the source buffer
     * @param remainp - pointer to remaining size variable
     * @return value
     */
    inline uint32_t decode_vi32(const uint8_t **bufp, size_t *remainp) {
      HT_DECODE_VINT0(uint32_t, n, *bufp, remainp)
      HT_DECODE_VINT4(uint32_t, n, *bufp, remainp)
      HT_DECODE_VINT_(uint32_t, n, *bufp, remainp)
      throw Exception(Error::SERIALIZATION_BAD_VINT);
    }

    /**
     * Decode a variable length encoded integer up to 64-bit
     *
     * @param bufp - pointer to the source buffer
     * @param remainp - pointer to remaining size variable
     * @return value
     */
    inline uint64_t decode_vi64(const uint8_t **bufp, size_t *remainp) {
      HT_DECODE_VINT0(uint64_t, n, *bufp, remainp)
      HT_DECODE_VINT4(uint64_t, n, *bufp, remainp)
      HT_DECODE_VINT4(uint64_t, n, *bufp, remainp)
      HT_DECODE_VINT_(uint64_t, n, *bufp, remainp)
      HT_DECODE_VINT_(uint64_t, n, *bufp, remainp)
      throw Exception(Error::SERIALIZATION_BAD_VINT);
    }


    /**
     * Computes the variable encoded length of a c-style null-terminated string
     * Assuming string length can be encoded in 32-bit integer
     *
     * @param str pointer to the the c-style string
     * @return the encoded length of str
     */
    inline size_t encoded_length_cstr(const char *s) {
      uint32_t len = s ? strlen(s) : 0;
      return 1 + len + encoded_length_vi32(len);
    }


    /**
     * Encode a c-style string a long with the terminating null
     *
     * @param bufp - pointer to destination buffer
     * @param s - pointer to the start of the string
     */
    inline void encode_cstr(uint8_t **bufp, const char *s) {
      uint32_t len = s ? strlen(s) : 0;

      encode_vi32(bufp, len);

      if (len) {
        memcpy(*bufp, s, len);
        *bufp += len;
      }
      *(*bufp)++ = 0;
    }


    /**
     * Decode a null terminated c-style string.
     *
     * @param bufp - pointer to the source buffer
     * @param remainp - pointer to the remaining size variable
     * @return str
     */
    inline const char *decode_cstr(const uint8_t **bufp, size_t *remainp) {
      uint32_t len = decode_vi32(bufp, remainp) + 1;
      const char *buf = (const char *)*bufp;

      if (*remainp < len)
        throw Exception(Error::SERIALIZATION_INPUT_OVERRUN);

      *bufp += len;
      *remainp -= len;

      if ((*bufp)[-1]) // should be null
        throw Exception(Error::SERIALIZATION_BAD_CSTR);

      return buf;
    }


}} // namespace Hypertable::Serialization

#endif // HYPERTABLE_SERIALIZATION_H
