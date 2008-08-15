/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_KEY_H
#define HYPERTABLE_KEY_H

#include <iostream>

#include <boost/shared_array.hpp>
#include <boost/detail/endian.hpp>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"

#include "KeySpec.h"
#include "SerializedKey.h"


namespace Hypertable {

  static const uint32_t FLAG_DELETE_ROW            = 0x00;
  static const uint32_t FLAG_DELETE_COLUMN_FAMILY  = 0x01;
  static const uint32_t FLAG_DELETE_CELL           = 0x02;
  static const uint32_t FLAG_INSERT                = 0xFF;

  /** Provides access to internal components of opaque key.
   */
  class Key {
  public:

    static const uint8_t HAVE_REVISION  = 0x80;
    static const uint8_t HAVE_TIMESTAMP = 0x40;
    static const uint8_t AUTO_TIMESTAMP = 0x20;
    static const uint8_t REV_IS_TS      = 0x10;

    static const char *END_ROW_MARKER;
    static const char *END_ROOT_ROW;

    static inline void encode_ts64(uint8_t **bufp, int64_t val) {
      val = ~val;
#ifdef BOOST_LITTLE_ENDIAN
      *(*bufp)++ = (uint8_t)(val >> 56);
      *(*bufp)++ = (uint8_t)(val >> 48);
      *(*bufp)++ = (uint8_t)(val >> 40);
      *(*bufp)++ = (uint8_t)(val >> 32);
      *(*bufp)++ = (uint8_t)(val >> 24);
      *(*bufp)++ = (uint8_t)(val >> 16);
      *(*bufp)++ = (uint8_t)(val >> 8);
      *(*bufp)++ = (uint8_t)val;
#else
      memcpy(*bufp, &val, 8);
      *bufp += 8;
#endif
    }

    static inline int64_t decode_ts64(const uint8_t **bufp) {
      int64_t val;
#ifdef BOOST_LITTLE_ENDIAN
      val = ((int64_t)*(*bufp)++ << 56);
      val |= ((int64_t)(*(*bufp)++) << 48);
      val |= ((int64_t)(*(*bufp)++) << 40);
      val |= ((int64_t)(*(*bufp)++) << 32);
      val |= ((int64_t)(*(*bufp)++) << 24);
      val |= (*(*bufp)++ << 16);
      val |= (*(*bufp)++ << 8);
      val |= *(*bufp)++;
#else
      memcpy(&val, *bufp, 8);
      *bufp += 8;
#endif
      return ~val;
    }

    /**
     * Constructor (for implicit construction).
     */
    Key() : timestamp(0), revision(AUTO_ASSIGN), flag(FLAG_INSERT) { return; }

    /**
     * Constructor that takes an opaque key as an argument.  load is called to
     * load the object with the key's components.
     *
     * @param key the opaque key
     */
    Key(SerializedKey key);

    /**
     * Parses the opaque key and loads the components into the member variables
     *
     * @param key the opaque key
     * @return true on success, false otherwise
     */
    bool load(SerializedKey key);
    inline size_t len_row() const {
      return(strlen(row) + 1);
    }
    inline size_t len_column_family() const {
      return(column_qualifier - row);
    }
    inline size_t len_cell() const {
      return((column_qualifier - row) + strlen(column_qualifier) + 1);
    }

    SerializedKey  serial;
    uint32_t       length;

    const char    *row;
    const char    *column_qualifier;
    const uint8_t *flag_ptr;
    int64_t        timestamp;
    int64_t        revision;
    uint8_t        column_family_code;
    uint8_t        flag;
    uint8_t        control;
  };


  /**
   * Prints a one-line representation of the key to the given ostream.
   *
   * @param os output stream to print key to
   * @param key the key to print
   * @return the output stream
   */
  std::ostream &operator<<(std::ostream &os, const Key &key);

  /**
   * Prints a one-line representation of the key to the given ostream.
   */
  inline std::ostream &
  operator<<(std::ostream &os, const SerializedKey &serkey) {
    Key key(serkey);
    os << key;
    return os;
  }


  /**
   * Builds an opaque key from a set of key components.  This function allocates
   * memory for the key an then packs the components into the key so that keys
   * can be lexicographically compared.  The opaque key has the following
   * packed format:
   * <p>
   * [rowkey][column-family][column-qualifier][flag][~BIGENDIAN(timestamp)]
   * <p>
   * @param flag DELETE_ROW, DELETE_CELL, or INSERT
   * @param row NUL-terminated row key
   * @param column_family_code column family
   * @param column_qualifier NUL-terminated column qualifier
   * @param timestamp timestamp in microseconds
   * @param revision
   * @return newly allocated opaque key
   */
  ByteString create_key(uint8_t flag, const char *row,
                        uint8_t column_family_code,
                        const char *column_qualifier,
                        int64_t timestamp = AUTO_ASSIGN,
                        int64_t revision = AUTO_ASSIGN);

  void create_key_and_append(DynamicBuffer &dst_buf, const char *row);

  void create_key_and_append(DynamicBuffer &dst_buf, uint8_t flag,
                             const char *row, uint8_t column_family_code,
                             const char *column_qualifier,
                             int64_t timestamp = AUTO_ASSIGN,
                             int64_t revision = AUTO_ASSIGN);

} // namespace Hypertable

#endif // HYPERTABLE_KEY_H

