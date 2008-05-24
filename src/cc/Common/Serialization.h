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

#include <cstring>
#include <string>

#include <boost/detail/endian.hpp>

#include "Common/Error.h"
#include "Common/Logger.h"

#ifdef HT_SERIALIZATION_GENERIC
#undef BOOST_LITTLE_ENDIAN
#endif

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

#define HT_MAX_LEN_VINT32 5
#define HT_MAX_LEN_VINT64 10

// vint encode
#define HT_ENCODE_VINT0(_op_, _val_) \
  if (_val_ <= HT_MAX_V1B) { \
    *(_op_)++ = (uint8_t)((_val_) & 0x7f); \
    return; \
  }

#define HT_ENCODE_VINT_(_op_, _val_) \
  *(_op_)++ = (uint8_t)((_val_) | 0x80); \
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
     * @param bufp address of destination buffer
     * @param bval the boolean value
     */
    inline void encode_bool(uint8_t **bufp, bool bval) {
      *(*bufp) = (bval) ? 1 : 0;
      (*bufp)++;
    }


    /**
     * Decodes a boolean value from the given buffer.  Increments buffer pointer
     * and decrements remainingp on success.
     *
     * @param bufp address of source buffer
     * @param remainingp address of variable containing number of bytes remaining in buffer
     * @param boolp address of variable to hold decoded byte
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_bool(uint8_t **bufp, size_t *remainingp, bool *boolp) {
      if (*remainingp == 0)
	return false;
      *boolp = (*(*bufp) == 0) ? false : true;
      (*bufp)++;
      (*remainingp)--;
      return true;
    }


    /**
     * Decodes a single byte from the given buffer.  Increments buffer pointer
     * and decrements remainingp on success.
     *
     * @param bufp address of source buffer
     * @param remainingp address of variable containing number of bytes remaining in buffer
     * @param bytePtr address of variable to hold decoded byte
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_byte(uint8_t **bufp, size_t *remainingp, uint8_t *bytePtr) {
      if (*remainingp == 0)
	return false;
      *bytePtr = *(*bufp)++;
      (*remainingp)--;
      return true;
    }


    /**
     * Encodes a short (2 byte integer) into the given buffer.  Assumes there is
     * enough space available.  Increments buffer pointer.
     *
     * @param bufp address of destinatin buffer
     * @param sval the short value to encode
     */
    inline void encode_short(uint8_t **bufp, uint16_t sval) {
#ifdef BOOST_LITTLE_ENDIAN
      memcpy(*bufp, &sval, 2);
      *bufp += 2;
#else
      *(*bufp)++ = (uint8_t)sval;
      *(*bufp)++ = (uint8_t)(sval >> 8);
#endif
    }


    /**
     * Decodes a short (2 byte integer) from the given buffer.  Increments
     * buffer pointer and decrements remainingp on success.
     *
     * @param bufp address of buffer containing encoded short
     * @param remainingp address of variable containing number of bytes remaining in buffer
     * @param svalp address of variable to hold decoded short
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_short(uint8_t **bufp, size_t *remainingp, uint16_t *svalp) {
      if (*remainingp < 2)
	return false;
#ifdef BOOST_LITTLE_ENDIAN
      memcpy(svalp, *bufp, 2);
      *bufp += 2;
#else
      *svalp = *(*bufp)++;
      *svalp |= (*(*bufp)++ << 8);
#endif
      *remainingp -= 2;
      return true;
    }


    /**
     * Encodes an int (4 byte integer) into the given buffer.  Assumes there is
     * enough space available.  Increments buffer pointer.
     *
     * @param bufp address of destinatin buffer
     * @param ival the int value to encode
     */
    inline void encode_int(uint8_t **bufp, uint32_t ival) {
#ifdef BOOST_LITTLE_ENDIAN
      memcpy(*bufp, &ival, 4);
      *bufp += 4;
#else
      *(*bufp)++ = (uint8_t)ival;
      *(*bufp)++ = (uint8_t)(ival >> 8);
      *(*bufp)++ = (uint8_t)(ival >> 16);
      *(*bufp)++ = (uint8_t)(ival >> 24);
#endif
    }


    /**
     * Decodes an int (4 byte integer) from the given buffer.  Increments
     * buffer pointer and decrements remainingp on success.
     *
     * @param bufp address of buffer containing encoded int
     * @param remainingp address of variable containing number of bytes remaining in buffer
     * @param ivalp address of variable to hold decoded int
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_int(uint8_t **bufp, size_t *remainingp, uint32_t *ivalp) {
      if (*remainingp < 4)
	return false;

#ifdef BOOST_LITTLE_ENDIAN
      memcpy(ivalp, *bufp, 4);
      *bufp += 4;
#else
      *ivalp = *(*bufp)++;
      *ivalp |= (*(*bufp)++ << 8);
      *ivalp |= (*(*bufp)++ << 16);
      *ivalp |= (*(*bufp)++ << 24);
#endif
      *remainingp -= 4;
      return true;
    }


    /**
     * Encodes a long (8 byte integer) into the given buffer.  Assumes there is
     * enough space available.  Increments buffer pointer.
     *
     * @param bufp address of destinatin buffer
     * @param lval the long value to encode
     */
    inline void encode_long(uint8_t **bufp, uint64_t lval) {
#ifdef BOOST_LITTLE_ENDIAN
      memcpy(*bufp, &lval, 8);
      *bufp += 8;
#else
      *(*bufp)++ = (uint8_t)lval;
      *(*bufp)++ = (uint8_t)(lval >> 8);
      *(*bufp)++ = (uint8_t)(lval >> 16);
      *(*bufp)++ = (uint8_t)(lval >> 24);
      *(*bufp)++ = (uint8_t)(lval >> 32);
      *(*bufp)++ = (uint8_t)(lval >> 40);
      *(*bufp)++ = (uint8_t)(lval >> 48);
      *(*bufp)++ = (uint8_t)(lval >> 56);
#endif
    }


    /**
     * Decodes an int (4 byte integer) from the given buffer.  Increments
     * buffer pointer and decrements remainingp on success.
     *
     * @param bufp address of buffer containing encoded long
     * @param remainingp address of variable containing number of bytes remaining in buffer
     * @param lvalp address of variable to hold decoded long
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_long(uint8_t **bufp, size_t *remainingp, uint64_t *lvalp) {
      if (*remainingp < 8)
	return false;

#ifdef BOOST_LITTLE_ENDIAN
      memcpy(lvalp, *bufp, 8);
      *bufp += 8;
#else
      *lvalp = *(*bufp)++;
      *lvalp |= (*(*bufp)++ << 8);
      *lvalp |= (*(*bufp)++ << 16);
      *lvalp |= ((uint64_t)(*(*bufp)++) << 24);
      *lvalp |= ((uint64_t)(*(*bufp)++) << 32);
      *lvalp |= ((uint64_t)(*(*bufp)++) << 40);
      *lvalp |= ((uint64_t)(*(*bufp)++) << 48);
      *lvalp |= ((uint64_t)(*(*bufp)++) << 56);
#endif

      *remainingp -= 8;
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
     * @param bufp address of destinatin buffer
     * @param data pointer to array of bytes
     * @param len the length of the byte array
     */
    inline void encode_byte_array(uint8_t **bufp, const void *data, int32_t len) {
      encode_int(bufp, len);
      memcpy(*bufp, (uint8_t *)data, len);
      *bufp += len;
    }


    /**
     * Decodes a variable sized byte array from the given buffer.  Byte array is
     * encoded as a 4 byte length followed by the data.  Increments buffer
     * pointer and decrements remainingp on success.
     *
     * @param bufp address of buffer containing encoded byte array
     * @param remainingp address of variable containing number of bytes remaining in buffer
     * @param dstPtr address of pointer to decoded byte array
     * @param lenp address of length of decoded byte array
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_byte_array(uint8_t **bufp, size_t *remainingp, uint8_t **dstPtr, int32_t *lenp) {
      // length
      if (*remainingp < 4)
	return false;
      if (!decode_int(bufp, remainingp, (uint32_t *)lenp))
	return false;
      // data
      if (*remainingp < (size_t)*lenp)
	return false;
      *dstPtr = (*bufp);
      *remainingp -= *lenp;
      *bufp += *lenp;
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
     * @param bufp address of destination buffer
     * @param str the c-style string to encode
     */
    inline void encode_string(uint8_t **bufp, const char *str) {
      uint16_t len = (str == 0) ? 0 : strlen(str);

      encode_short(bufp, len);

      // string characters
      if (len > 0) {
	memcpy(*bufp, str, len);
	(*bufp) += len;
      }

      // '\\0' terminator
      *(*bufp)++ = 0;
    }


    /**
     * Encodes a std::string into the given buffer.  Encoded as a
     * 2 byte length followed by the string data, followed by a '\\0'
     * termination byte.  The length value does not include the '\\0'.
     * Assumes there is enough space available.  Increments buffer
     * pointer.
     *
     * @param bufp address of destinatin buffer
     * @param str the std::string to encode
     */
    inline void encode_string(uint8_t **bufp, const std::string &str) {
      encode_string(bufp, str.c_str());
    }


    /**
     * Decodes a c-style string from the given buffer.  The encoding of the
     * string is a 2 byte length followed by the string, followed by a '\\0'
     * termination byte.  The length does not include the '\\0' terminator.
     * The decoded string pointer points back into the encoding buffer.
     * Increments buffer pointer and decrements remainingp on success.
     *
     * @param bufp address of buffer containing encoded string
     * @param remainingp address of variable containing number of bytes remaining in buffer
     * @param strp address of string pointer, points back into encoding buffer on success
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_string(uint8_t **bufp, size_t *remainingp, const char **strp) {
      uint16_t len;
      *strp = 0;
      if (*remainingp < 3)
	return false;
      if (!decode_short(bufp, remainingp, &len))
	return false;
      if (*remainingp < (size_t)len+1)
	return false;
      *strp = (const char *)*bufp;
      *remainingp -= (size_t)len+1;
      *bufp += (size_t)len+1;
      return true;
    }


    /**
     * Decodes a std::string from the given buffer.  The encoding of the
     * string is a 2 byte length followed by the string, followed by a '\\0'
     * termination byte.  The length does not include the '\\0' terminator.
     * Increments buffer pointer and decrements remainingp on success.
     *
     * @param bufp address of buffer containing encoded string
     * @param remainingp address of variable containing number of bytes remaining in buffer
     * @param str reference to std::string to hold result
     * @return true on success, false if buffer has insufficient room
     */
    inline bool decode_string(uint8_t **bufp, size_t *remainingp, std::string &str) {
      const char *cstr;
      if (!decode_string(bufp, remainingp, &cstr))
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
     * Encode a 16-bit integer in little-endian order 
     *
     * @param bufp - pointer to the destination buffer
     * @param val value to encode
     */
    inline void encode_i16(uint8_t **bufp , uint16_t val) {
#ifdef BOOST_LITTLE_ENDIAN
      memcpy(*bufp, &val, 2);
      *bufp += 2;
#else
      *(*bufp)++ = (uint8_t)val;
      *(*bufp)++ = (uint8_t)(val >> 8);
#endif
    }


    /**
     * Decode a 16-bit integer in little-endian order
     *
     * @param bufp - pointer to the source buffer
     * @param remainp - pointer to remaining size variable
     * @return value
     */
    inline uint16_t decode_i16(const uint8_t **bufp, size_t *remainp) {
      if (*remainp < 2)
        throw Exception(Error::SERIALIZATION_INPUT_OVERRUN);

#ifdef BOOST_LITTLE_ENDIAN
      uint16_t val;
      memcpy(&val, *bufp, 2);
      *bufp += 2;
#else
      uint16_t val = *(*bufp)++;
      val |= (*(*bufp)++ << 8);
#endif
      *remainp -= 2;

      return val;
    }


    /**
     * Encode a 32-bit integer in little-endian order
     *
     * @param bufp - pointer to the destination buffer
     * @param val - value to encode
     */
    inline void encode_i32(uint8_t **bufp, uint32_t val) {
#ifdef BOOST_LITTLE_ENDIAN
      memcpy(*bufp, &val, 4);
      *bufp += 4;
#else
      *(*bufp)++ = (uint8_t)val;
      *(*bufp)++ = (uint8_t)(val >> 8);
      *(*bufp)++ = (uint8_t)(val >> 16);
      *(*bufp)++ = (uint8_t)(val >> 24);
#endif
    }


    /**
     * Decode a 32-bit integer in little-endian order
     *
     * @param bufp - pointer to the source buffer
     * @param remainp - pointer to remaining size variable
     * @return value
     */
    inline uint32_t decode_i32(const uint8_t **bufp, size_t *remainp) {
      if (*remainp < 4)
        throw Exception(Error::SERIALIZATION_INPUT_OVERRUN);

#ifdef BOOST_LITTLE_ENDIAN
      uint32_t val;
      memcpy(&val, *bufp, 4);
      *bufp += 4;
#else
      uint32_t val = *(*bufp)++;
      val |= (*(*bufp)++ << 8);
      val |= (*(*bufp)++ << 16);
      val |= (*(*bufp)++ << 24);
#endif
      *remainp -= 4;

      return val;
    }


    /**
     * Encode a 64-bit integer in little-endian order
     *
     * @param bufp - pointer to the destination buffer
     * @param val - value to encode
     */
    inline void encode_i64(uint8_t **bufp, uint64_t val) {
#ifdef BOOST_LITTLE_ENDIAN
      memcpy(*bufp, &val, 8);
      *bufp += 8;
#else
      *(*bufp)++ = (uint8_t)val;
      *(*bufp)++ = (uint8_t)(val >> 8);
      *(*bufp)++ = (uint8_t)(val >> 16);
      *(*bufp)++ = (uint8_t)(val >> 24);
      *(*bufp)++ = (uint8_t)(val >> 32);
      *(*bufp)++ = (uint8_t)(val >> 40);
      *(*bufp)++ = (uint8_t)(val >> 48);
      *(*bufp)++ = (uint8_t)(val >> 56);
#endif
    }


    /**
     * Decode a 64-bit integer in little-endian order
     *
     * @param bufp - pointer to the source buffer
     * @param remainp - pointer to remaining size variable
     * @return value
     */
    inline uint64_t decode_i64(const uint8_t **bufp, size_t *remainp) {
      if (*remainp < 8)
        throw Exception(Error::SERIALIZATION_INPUT_OVERRUN);

#ifdef BOOST_LITTLE_ENDIAN
      uint64_t val;
      memcpy(&val, *bufp, 8);
      *bufp += 8;
#else
      uint64_t val = *(*bufp)++;
      val |= (*(*bufp)++ << 8);
      val |= (*(*bufp)++ << 16);
      val |= ((uint64_t)(*(*bufp)++) << 24);
      val |= ((uint64_t)(*(*bufp)++) << 32);
      val |= ((uint64_t)(*(*bufp)++) << 40);
      val |= ((uint64_t)(*(*bufp)++) << 48);
      val |= ((uint64_t)(*(*bufp)++) << 56);
#endif
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
      return (val <= HT_MAX_V1B ? 1 : \
	      (val <= HT_MAX_V2B ? 2 : \
	       (val <= HT_MAX_V3B ? 3 : \
		(val <= HT_MAX_V4B ? 4 : 5))));
    }

    /**
     * Length of a variable length encoded 64-bit integer (up to 9 bytes)
     *
     * @param val - 64-bit integer to encode
     * @return number of bytes required
     */
    inline int encoded_length_vi64(uint64_t val) {
      return (val <= HT_MAX_V1B ? 1 : \
	      (val <= HT_MAX_V2B ? 2 : \
	       (val <= HT_MAX_V3B ? 3 : \
		(val <= HT_MAX_V4B ? 4 : \
		 (val <= HT_MAX_V5B ? 5 : \
		  (val <= HT_MAX_V6B ? 6 : \
		   (val <= HT_MAX_V7B ? 7 : \
		    (val <= HT_MAX_V8B ? 8 : \
		     (val <= HT_MAX_V9B ? 9 : 10)))))))));
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
      HT_EXPECT(!"corrupted vint32", Error::FAILED_EXPECTATION);
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
      HT_EXPECT(!"corrupted vint64", Error::FAILED_EXPECTATION);
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
     * Computes the variable encoded length of a c-style null-terminated string
     * Assuming string length can be encoded in 32-bit integer
     *
     * @param s pointer to the the c-style string
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
