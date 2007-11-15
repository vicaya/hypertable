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

#ifndef HYPERSPACE_DIRENTRY_H
#define HYPERSPACE_DIRENTRY_H

#include <string>

namespace Hyperspace {

  /**
   * Listing information for each node within a directory.  A vector
   * of these objects gets passed back to the application via a call to
   * Readdir()
   */
  struct DirEntryT {
    /** Directory entry name. */
    std::string name;
    /** Boolean value indicating whether or not this entry is a directory */
    bool isDirectory;
  };

  struct ltDirEntry {
    bool operator()(const struct DirEntryT &de1, const struct DirEntryT &de2) const {
      if (de1.name == de2.name)
	return (int)de1.isDirectory < (int)de2.isDirectory;
      return de1.name < de2.name;
    }
  };

  /** Returns the number of bytes required to encode (serialize) the given
   * directory entry.
   *
   * @param dir_entry the directory entry
   * @return the exact number of bytes required to encode dir_entry
   */
  size_t encoded_length_dir_entry(DirEntryT &dir_entry);

  /** Encodes (serializes) the given directory entry to a buffer.
   *
   * @param buf_ptr address of pointer to buffer to receive encoded directory entry (pointer is advanced passed the encoded entry)
   * @param dir_entry the directory entry to encode
   */
  void encode_dir_entry(uint8_t **buf_ptr, DirEntryT &dir_entry);

  /** Decodes (unserializes) a directory entry from a buffer
   * 
   * @param buf_ptr address of pointer to buffer containing encoded directory entry (advanced after decode)
   * @param remaining_ptr address of count variable holding the number of bytes remaining in buffer (decremented after decode)
   * @param dir_entry the directory entry to encode
   */
  bool decode_range_dir_entry(uint8_t **buf_ptr, size_t *remaining_ptr, DirEntryT &dir_entry);
  
}

#endif // HYPERSPACE_DIRENTRY_H
