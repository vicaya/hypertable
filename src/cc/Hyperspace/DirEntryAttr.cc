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
#include "Common/DynamicBuffer.h"

#include "DirEntryAttr.h"

using namespace Hypertable;
using namespace Serialization;
using namespace boost;

namespace Hyperspace {

  size_t encoded_length_dir_entry_attr(const DirEntryAttr &entry) {
    size_t len = 2 + encoded_length_vstr(entry.name) + 4 + entry.attr.size + 4;
    foreach(const DirEntryAttr &sub_entry, entry.sub_entries) 
      len += encoded_length_dir_entry_attr(sub_entry);
    return len;
  }

  void encode_dir_entry_attr(uint8_t **bufp, const DirEntryAttr &entry) {
    encode_bool(bufp, entry.has_attr);
    encode_bool(bufp, entry.is_dir);
    encode_vstr(bufp, entry.name);
    encode_bytes32(bufp, (void *)entry.attr.base, entry.attr.size);
    encode_i32(bufp, entry.sub_entries.size());
    foreach(const DirEntryAttr &sub_entry, entry.sub_entries) 
      encode_dir_entry_attr(bufp, sub_entry);
  }

  DirEntryAttr &
  decode_dir_entry_attr(const uint8_t **bufp, size_t *remainp, DirEntryAttr &entry) {
    try {
      entry.has_attr = decode_bool(bufp, remainp);
      entry.is_dir = decode_bool(bufp, remainp);
      entry.name = decode_vstr(bufp, remainp);
      uint32_t attr_val_len;
      void *attr_val= decode_bytes32(bufp, remainp, &attr_val_len);

      DynamicBuffer buffer(attr_val_len+1);
      buffer.add_unchecked(attr_val, attr_val_len);
      // nul-terminate to make caller's lives easier
      *buffer.ptr = 0;
      buffer.size = attr_val_len+1;
      entry.attr = buffer;

      DirEntryAttr sub_entry;
      uint32_t sub_entries_count = decode_i32(bufp, remainp);
      entry.sub_entries.clear();
      entry.sub_entries.reserve(sub_entries_count);
      while (sub_entries_count--) {
        entry.sub_entries.push_back(decode_dir_entry_attr(bufp, remainp, sub_entry));
      }
    }
    catch (Exception &e) {
      HT_THROW2(e.code(), e, "Error deserializing DirEntryAttr");
    }
    return entry;
  }
}
