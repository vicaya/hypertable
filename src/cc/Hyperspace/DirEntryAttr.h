/**
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
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

#ifndef HYPERSPACE_DIRENTRYATTR_H
#define HYPERSPACE_DIRENTRYATTR_H

#include "Common/Compat.h"

#include <string>
#include "Common/StaticBuffer.h"

namespace Hyperspace {

  using namespace Hypertable;

  /**
   * Listing information for each node within a directory.  A vector
   * of these objects gets passed back to the application via a call to
   * Readdirfileattr()
   */
  class DirEntryAttr {
  public:

    DirEntryAttr() {}
    // This copy constructor violates the const-ness of the parameter since
    // this attr takes ownership of the other attr StaticBuffer
    DirEntryAttr(const DirEntryAttr &other):name(other.name), is_dir(other.is_dir) {
      attr = (const_cast<DirEntryAttr&>(other)).attr;
    }

    std::string name;
    StaticBuffer attr;
    /** Boolean value indicating whether or not this entry is a directory */
    bool is_dir;
  };

  struct LtDirEntryAttr {
    bool operator()(const struct DirEntryAttr &x, const struct DirEntryAttr &y) const {
      if (x.name == y.name)
        return (int)x.is_dir < (int)y.is_dir;
      return x.name < y.name;
    }
  };

  /** Returns the number of bytes required to encode (serialize) the given
   * directory entry.
   *
   * @param entry the directory entry
   * @return the exact number of bytes required to encode entry
   */
  size_t encoded_length_dir_entry_attr(const DirEntryAttr &entry);

  /** Encodes (serializes) the given directory entry to a buffer.
   *
   * @param buf_ptr address of pointer to buffer to receive encoded directory
   *        entry (pointer is advanced passed the encoded entry)
   * @param entry the directory entry to encode
   */
  void encode_dir_entry_attr(uint8_t **buf_ptr, const DirEntryAttr &entry);

  /** Decodes (unserializes) a directory entry from a buffer
   *
   * @param buf_ptr address of pointer to buffer containing encoded directory
   *        entry (advanced after decode)
   * @param remaining_ptr address of count variable holding the number of bytes
   *        remaining in buffer (decremented after decode)
   * @param entry the directory entry to encode
   */
  DirEntryAttr &decode_dir_entry_attr(const uint8_t **buf_ptr, size_t *remaining_ptr,
                                      DirEntryAttr &entry);

}

#endif // HYPERSPACE_DIRENTRYATTR_H
