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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include "Common/Serialization.h"
#include "Common/Logger.h"

#include "DirEntry.h"

using namespace Hypertable;
using namespace Serialization;

namespace Hyperspace {

  size_t encoded_length_dir_entry(const DirEntry &dir_entry) {
    return 1 + encoded_length_vstr(dir_entry.name);
  }

  void encode_dir_entry(uint8_t **bufp, const DirEntry &dir_entry) {
    encode_bool(bufp, dir_entry.is_dir);
    encode_vstr(bufp, dir_entry.name);
  }

  DirEntry &
  decode_dir_entry(const uint8_t **bufp, size_t *remainp,
                         DirEntry &dir_entry) {
    HT_TRY("decoding dir entry",
      dir_entry.is_dir = decode_bool(bufp, remainp);
      dir_entry.name = decode_vstr(bufp, remainp));
    return dir_entry;
  }
}
