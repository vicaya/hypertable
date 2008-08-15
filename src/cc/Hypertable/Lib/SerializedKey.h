/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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

#ifndef HYPERTABLE_SERIALIZEDKEY_H
#define HYPERTABLE_SERIALIZEDKEY_H

#include "Common/ByteString.h"
#include "Common/Logger.h"

namespace Hypertable {

  class SerializedKey : public ByteString {
  public:
    SerializedKey() {}
    SerializedKey(const uint8_t *buf) : ByteString(buf) { }
    SerializedKey(ByteString bs) { ptr = bs.ptr; }

    int compare(const SerializedKey sk) const {
      const uint8_t *ptr1, *ptr2;
      int len1 = decode_length(&ptr1);
      int len2 = sk.decode_length(&ptr2);

      // common case
      if (*ptr1 != *ptr2) {
        if ( *ptr1 == 0xD0 ) // see Key.h
          len2 -= 8;
        else if ( *ptr2 == 0xD0 ) // see Key.h
          len1 -= 8;
        else {
          len1 -= 8;
          len2 -= 8;
        }
      }
      int len = (len1 < len2) ? len1 : len2;
      int cmp = memcmp(ptr1+1, ptr2+1, len-1);
      return (cmp==0) ? len1 - len2 : cmp;
    }

    const char *row() const {
      const uint8_t *rptr = ptr;
      Serialization::decode_vi32(&rptr);
      return (const char *)rptr+1;
    }
  };

  inline bool operator==(const SerializedKey sk1, const SerializedKey sk2) {
    return sk1.compare(sk2) == 0;
  }

  inline bool operator!=(const SerializedKey sk1, const SerializedKey sk2) {
    return sk1.compare(sk2) != 0;
  }

  inline bool operator<(const SerializedKey sk1, const SerializedKey sk2) {
    return sk1.compare(sk2) < 0;
  }

  inline bool operator<=(const SerializedKey sk1, const SerializedKey sk2) {
    return sk1.compare(sk2) <= 0;
  }

  inline bool operator>(const SerializedKey sk1, const SerializedKey sk2) {
    return sk1.compare(sk2) > 0;
  }

  inline bool operator>=(const SerializedKey sk1, const SerializedKey sk2) {
    return sk1.compare(sk2) >= 0;
  }


}

#endif // HYPERTABLE_SERIALIZEDKEY_H
