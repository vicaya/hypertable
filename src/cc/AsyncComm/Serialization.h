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

#ifndef HYPERTABLE_SERIALIZATION_H
#define HYPERTABLE_SERIALIZATION_H

#include <cstring>
#include <string>

#include "Common/ByteString.h"

namespace hypertable {

  namespace Serialization {

    /**
     * Decodes a single byte from the given buffer.  Increments buffer pointer
     * and decrements remainingPtr on success.
     *
     * @param bufPtr address of source buffer
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @return true on success, false if buffer has insufficient room
     */
    inline bool DecodeByte(uint8_t **bufPtr, size_t *remainingPtr, uint8_t *bytePtr) {
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
    inline void EncodeShort(uint8_t **bufPtr, uint16_t sval) {
      memcpy(*bufPtr, &sval, 2);
      (*bufPtr) += 2;
    }


    /**
     * Decodes a short (2 byte integer) from the given buffer.  Increments
     * buffer pointer and decrements remainingPtr on success.
     *
     * @param bufPtr address of buffer containing encoded short
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @return true on success, false if buffer has insufficient room
     */
    inline bool DecodeShort(uint8_t **bufPtr, size_t *remainingPtr, uint16_t *shortPtr) {
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
    inline void EncodeInt(uint8_t **bufPtr, uint32_t ival) {
      memcpy(*bufPtr, &ival, 4);
      *bufPtr += 4;
    }


    /**
     * Decodes an int (4 byte integer) from the given buffer.  Increments
     * buffer pointer and decrements remainingPtr on success.
     *
     * @param bufPtr address of buffer containing encoded int
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @return true on success, false if buffer has insufficient room
     */
    inline bool DecodeInt(uint8_t **bufPtr, size_t *remainingPtr, uint32_t *intPtr) {
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
     * @param ival the long value to encode
     */
    inline void EncodeLong(uint8_t **bufPtr, uint64_t lval) {
      memcpy(*bufPtr, &lval, 8);
      *bufPtr += 8;
    }


    /**
     * Decodes an int (4 byte integer) from the given buffer.  Increments
     * buffer pointer and decrements remainingPtr on success.
     *
     * @param bufPtr address of buffer containing encoded long
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @return true on success, false if buffer has insufficient room
     */
    inline bool DecodeLong(uint8_t **bufPtr, size_t *remainingPtr, uint64_t *longPtr) {
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
    inline size_t EncodedLengthByteArray(int32_t len) {
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
    inline void EncodeByteArray(uint8_t **bufPtr, const void *data, int32_t len) {
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
     * @return true on success, false if buffer has insufficient room
     */
    inline bool DecodeByteArray(uint8_t **bufPtr, size_t *remainingPtr, uint8_t **dstPtr, int32_t *lenPtr) {
      // length
      if (*remainingPtr < 4)
	return false;
      memcpy(lenPtr, *bufPtr, 4);
      (*remainingPtr) -= 4;
      (*bufPtr) += 4;
      // data
      if (*remainingPtr < *lenPtr)
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
    inline bool DecodeByteString32(const uint8_t **bufPtr, size_t *remainingPtr, ByteString32T **bs32Ptr) {
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
    inline size_t EncodedLengthString(const char *str) {
      return 2 + ((str == 0) ? 0 : strlen(str)) + 1;
    }


    /**
     * Computes the encoded length of a std::string.
     *
     * @param str reference to string object
     * @return the encoded length of str
     */
    inline size_t EncodedLengthString(const std::string &str) {
      return EncodedLengthString(str.c_str());
    }


    /**
     * Encodes a c-style null-terminated string into the given buffer.
     * Encoded as a 2 byte length followed by the string data, followed
     * by a '\0' termination byte.  The length value does not include
     * the '\0'.  Assumes there is enough space available.  Increments
     * buffer pointer.
     *
     * @param bufPtr address of destinatin buffer
     * @param str the c-style string to encode
     */
    inline void EncodeString(uint8_t **bufPtr, const char *str) {
      uint16_t len = (str == 0) ? 0 : strlen(str);

      // 2-byte length
      memcpy(*bufPtr, &len, 2);
      (*bufPtr) += 2;
      
      // string characters
      if (len > 0) {
	memcpy(*bufPtr, str, len);
	(*bufPtr) += len;
      }

      // '\0' terminator
      *(*bufPtr)++ = 0;
    }


    /**
     * Encodes a std::string into the given buffer.  Encoded as a
     * 2 byte length followed by the string data, followed by a '\0'
     * termination byte.  The length value does not include the '\0'.
     * Assumes there is enough space available.  Increments buffer
     * pointer.
     *
     * @param bufPtr address of destinatin buffer
     * @param str the std::string to encode
     */
    inline void EncodeString(uint8_t **bufPtr, const std::string &str) {
      EncodeString(bufPtr, str.c_str());
    }


    /**
     * Decodes a c-style string from the given buffer.  The encoding of the
     * string is a 2 byte length followed by the string, followed by a '\0'
     * termination byte.  The length does not include the '\0' terminator.
     * The decoded string pointer points back into the encoding buffer.
     * Increments buffer pointer and decrements remainingPtr on success.
     *
     * @param bufPtr address of buffer containing encoded string
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @param strPtr address of string pointer, points back into encoding buffer on success
     * @return true on success, false if buffer has insufficient room
     */
    inline bool DecodeString(uint8_t **bufPtr, size_t *remainingPtr, const char **strPtr) {
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
     * string is a 2 byte length followed by the string, followed by a '\0'
     * termination byte.  The length does not include the '\0' terminator.
     * Increments buffer pointer and decrements remainingPtr on success.
     *
     * @param bufPtr address of buffer containing encoded string
     * @param remainingPtr address of variable containing number of bytes remaining in buffer
     * @param str reference to std::string to hold result
     * @return true on success, false if buffer has insufficient room
     */
    inline bool DecodeString(uint8_t **bufPtr, size_t *remainingPtr, std::string &str) {
      const char *cstr;
      if (!DecodeString(bufPtr, remainingPtr, &cstr))
	return false;
      str = cstr;
      return true;
    }

  }
  
}

#endif // HYPERTABLE_SERIALIZATION_H
