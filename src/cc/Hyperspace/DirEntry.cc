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

#include "AsyncComm/Serialization.h"

#include "DirEntry.h"

using namespace Hypertable;

namespace Hyperspace {

  size_t encoded_length_dir_entry(DirEntryT &dir_entry) {
    return 1 + Serialization::encoded_length_string(dir_entry.name);
  }

  void encode_dir_entry(uint8_t **buf_ptr, DirEntryT &dir_entry) {
    uint8_t bval = (dir_entry.isDirectory) ? 1 : 0;
    *(*buf_ptr)++ = bval;
    Serialization::encode_string(buf_ptr, dir_entry.name);
  }

  bool decode_range_dir_entry(uint8_t **buf_ptr, size_t *remaining_ptr, DirEntryT &dir_entry) {
    uint8_t bval;
    if (!Serialization::decode_byte(buf_ptr, remaining_ptr, &bval))
      return false;
    dir_entry.isDirectory = (bval == 0) ? false : true;
    return Serialization::decode_string(buf_ptr, remaining_ptr, dir_entry.name);
  }
}
