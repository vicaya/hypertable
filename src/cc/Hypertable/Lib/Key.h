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

#ifndef HYPERTABLE_KEY_H
#define HYPERTABLE_KEY_H

#include <iostream>

#include <boost/shared_array.hpp>

#include "Common/ByteString.h"
#include "Common/DynamicBuffer.h"

using namespace Hypertable;

namespace Hypertable {

  static const uint32_t FLAG_DELETE_ROW            = 0x00;
  static const uint32_t FLAG_DELETE_COLUMN_FAMILY  = 0x01;
  static const uint32_t FLAG_DELETE_CELL           = 0x02;
  static const uint32_t FLAG_INSERT                = 0xFF;
  
  /** Provides access to internal components of opaque key.
   */
  class Key {
  public:

    static const char *END_ROW_MARKER;
    static const char *END_ROOT_ROW;

    /**
     * Constructor (for implicit construction).
     */
    Key() : timestamp_ptr(0) { return; }

    /**
     * Constructor that takes an opaque key as an argument.  load is called to
     * load the object with the key's components.
     *
     * @param key the opaque key
     */
    Key(const ByteString32T *key);

    /**
     * Parses the opaque key and loads the components into the member variables
     *
     * @param key the opaque key
     * @return true on success, false otherwise
     */
    bool load(const ByteString32T *key);

    /**
     * Updates the timestamp of a previously loaded key by writing the
     * key back into the serialized key.
     *
     * @param timestamp new timestamp 
     */
    void updateTimestamp(uint64_t timestamp);

    const char    *row;
    const char    *column_qualifier;
    uint64_t       timestamp;
    uint8_t        column_family_code;
    uint8_t        flag;
    uint8_t       *timestamp_ptr;
    const uint8_t *end_ptr;
  };


  /**
   * Prints a one-line representation of the key to the given ostream.
   * 
   * @param os output stream to print key to
   * @param key the key to print
   * @return the output stream
   */
  std::ostream &operator<<(std::ostream &os, const Key &key);


  void CreateKey(ByteString32T *key, uint8_t flag, const char *row, uint8_t column_family_code, const char *column_qualifier, uint64_t timestamp);

  /**
   * Builds an opaque key from a set of key components.  This function allocates
   * memory for the key an then packs the components into the key so that keys can
   * be lexicographically compared.  The opaque key has the following packed format:
   * <p>
   * &lt;rowKey&gt; NUL &lt;columnFamily&gt; &lt;columnQualifier&gt; NULL &lt;flag&gt; ~BIGENDIAN(&lt;timestamp&gt;)
   * <p>
   * @param flag DELETE_ROW, DELETE_CELL, or INSERT
   * @param row NUL-terminated row key
   * @param column_family_code column family
   * @param column_qualifier NUL-terminated column qualifier
   * @param timestamp timestamp in microseconds
   * @return newly allocated opaque key
   */
  ByteString32T *CreateKey(uint8_t flag, const char *row, uint8_t column_family_code, const char *column_qualifier, uint64_t timestamp);

  void CreateKeyAndAppend(DynamicBuffer &dst_buf, uint8_t flag, const char *row, uint8_t column_family_code, const char *column_qualifier, uint64_t timestamp);

}

#endif // HYPERTABLE_KEY_H

