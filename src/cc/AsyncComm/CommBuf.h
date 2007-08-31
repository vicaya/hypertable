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

namespace hypertable {

  /**
   * This class is used to carry buffers to be sent by the
   * AsyncComm subsystem.  It contains a pointer to a primary buffer
   * and an extended buffer and also contains buffer pointers
   * that keep track of how much data has already been written
   * in the case of partial writes.
   */
  class CommBuf {
  public:

    CommBuf(HeaderBuilder &hbuilder, uint32_t len, const uint8_t *_ex=0, uint32_t _exLen=0) {
      len += hbuilder.HeaderLength();
      data = dataPtr = new uint8_t [ len ];
      dataLen = len;
      ext = extPtr = _ex;
      extLen = _exLen;
      hbuilder.SetTotalLen(len);
      hbuilder.AssignUniqueId();
      hbuilder.Encode(&dataPtr);
    }

    CommBuf(uint32_t len) {
      data = dataPtr = new uint8_t [ len ];
      dataLen = len;
      ext = extPtr = 0;
      extLen = 0;
    }

    ~CommBuf() {
      delete [] data;
      delete [] ext;
    }

    void SetExt(const uint8_t *buf, uint32_t len) {
      ext = extPtr = (uint8_t *)buf;
      extLen = len;
    }

    void ResetDataPointers() {
      dataPtr = (uint8_t *)data;
      extPtr = ext;
    }

    void *GetDataPtr() { return dataPtr; }
    void *AdvanceDataPtr(size_t len) { dataPtr += len; }

    void AppendByte(uint8_t bval) { *dataPtr++ = bval; }
    void AppendBytes(uint8_t *bytes, uint32_t len) { memcpy(dataPtr, bytes, len); dataPtr += len; }
    void AppendShort(uint16_t sval) { Serialization::EncodeShort(&dataPtr, sval); }
    void AppendInt(uint32_t ival) { Serialization::EncodeInt(&dataPtr, ival); }
    void AppendLong(uint64_t lval) { Serialization::EncodeLong(&dataPtr, lval); }
    void AppendByteArray(const void *data, int32_t len) { Serialization::EncodeByteArray(&dataPtr, data, len); }
    void AppendString(const char *str) { Serialization::EncodeString(&dataPtr, str); }
    void AppendString(const std::string &str) { Serialization::EncodeString(&dataPtr, str.c_str()); }

    const uint8_t *data;
    uint8_t *dataPtr;
    uint32_t dataLen;
    const uint8_t *ext;
    const uint8_t *extPtr;
    uint32_t extLen;
  };

  typedef boost::shared_ptr<CommBuf> CommBufPtr;
  
}


#endif // HYPERTABLE_COMMBUF_H
