/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#ifndef HYPERTABLE_COMMBUF_H
#define HYPERTABLE_COMMBUF_H

#include <string>

extern "C" {
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
}

namespace hypertable {

  class CommBuf {
  public:

    CommBuf(uint32_t len) {
      bufLen = len;
      buf = new uint8_t [ bufLen ];
      data = buf + bufLen;
      dataLen = 0;
      ext = extPtr = 0;
      extLen = 0;
    }

    ~CommBuf() {
      delete [] buf;
      delete [] ext;
    }

    uint8_t *AllocateSpace(size_t len) {
      data -= len;
      dataLen += len;
      return data;
    }

    void PrependData(const void *dataBytes, size_t len) {
      data -= len;
      dataLen += len;
      memcpy(data, (uint8_t *)dataBytes, len);
    }

    void PrependShort(uint16_t sval) {
      data -= sizeof(int16_t);
      dataLen += sizeof(int16_t);
      memcpy(data, &sval, sizeof(int16_t));
    }

    void PrependInt(uint32_t ival) {
      data -= sizeof(int32_t);
      dataLen += sizeof(int32_t);
      memcpy(data, &ival, sizeof(int32_t));
    }

    void PrependByteArray(const void *dataBytes, int32_t len) {
      data -= len + sizeof(int32_t);
      dataLen += len + sizeof(int32_t);
      memcpy(data, &len, sizeof(int32_t));
      memcpy(data+sizeof(int32_t), (uint8_t *)dataBytes, len);
    }

    static size_t DecodeByteArray(const uint8_t *buf, size_t remain, const uint8_t **rptr, int32_t *lenp) {
      if (remain < sizeof(int32_t))
	return 0;
      memcpy(lenp, buf, sizeof(int32_t));
      if (remain < *lenp + sizeof(int32_t))
	return 0;
      *rptr = buf + sizeof(int32_t);
      return *lenp + sizeof(int32_t);
    }

    void PrependString(std::string &str) {
      PrependString(str.c_str());
    }

    void PrependString(const char *str) {
      uint16_t len = (str == 0) ? 0 : strlen(str);

      // '\0' terminator
      data--;
      dataLen++;
      *data = 0;

      // string characters
      if (len > 0) {
	data -= len;
	dataLen += len;
	memcpy(data, str, len);
      }

      // 2-byte length
      data -= sizeof(int16_t);
      dataLen += sizeof(int16_t);
      memcpy(data, &len, sizeof(int16_t));
    }

    // 2 byte length + string + '\0' terminator
    static size_t EncodedLength(std::string &str) {
      return EncodedLength(str.c_str());
    }

    // 2 byte length + string + '\0' terminator
    static size_t EncodedLength(const char *str) {
      return sizeof(int16_t) + ((str == 0) ? 0 : strlen(str)) + 1;
    }

    /**
     * Static method for decoding a string from a protocol packet.  Strings are
     * encoded as a 2-byte length followed by the string, followed by a '\0'
     * terminator.  The length does *not* include the '\0' terminator.
     *
     * @param buf    Pointer to beginning of encoded string
     * @param remain Number of valid bytes that buf points to
     * @param strp   Address of character pointer to hold return string pointer
     * @return Length of the encoded string
     */
    static size_t DecodeString(const uint8_t *buf, size_t remain, const char **strp) {
      uint16_t len;
      *strp = 0;
      if (remain < 3)
	return 0;
      memcpy(&len, buf, sizeof(uint16_t));
      if (remain < (size_t)len+3)
	return 0;
      *strp = (const char *)(buf+2);
      return len + 3;
    }

    static size_t DecodeString(const uint8_t *buf, size_t remain, std::string &str) {
      uint16_t len;
      str = "";
      if (remain < 3)
	return 0;
      memcpy(&len, buf, sizeof(uint16_t));
      if (remain < (size_t)len+3)
	return 0;
      if (len > 0) {
	str = (const char *)(buf+2);
      }
      return len + 3;
    }


    void SetExt(uint8_t *buf, uint32_t len) {
      ext = extPtr = buf;
      extLen = len;
    }

    uint8_t *data;
    uint32_t dataLen;
    uint8_t *ext;
    uint8_t *extPtr;
    uint32_t extLen;
    uint8_t *buf;
    uint32_t bufLen;
  };
}


#endif // HYPERTABLE_COMMBUF_H
