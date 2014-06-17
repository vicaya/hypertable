/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
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
#include "Common/Compat.h"
#include "Common/Filesystem.h"
#include "Common/Serialization.h"

#include <cctype>

#include "MetaLog.h"

using namespace Hypertable;
using namespace Hypertable::MetaLog;

void Header::encode(uint8_t **bufp) const {
  Serialization::encode_i16(bufp, version);
  memcpy(*bufp, name, 14);
  *bufp += 14;
}

void Header::decode(const uint8_t **bufp, size_t *remainp) {
  HT_ASSERT(*remainp >= LENGTH);
  version = Serialization::decode_i16(bufp, remainp);
  memcpy(name, *bufp, 14);
  *bufp += 14;
}


void MetaLog::scan_log_directory(FilesystemPtr &fs, const String &path,
                                 std::vector<int32_t> &file_ids, int32_t *nextidp) {
  const char *ptr;
  int32_t id;
  std::vector<String> listing;

  *nextidp = 0;

  if (!fs->exists(path))
    return;

  fs->readdir(path, listing);

  for (size_t i=0; i<listing.size(); i++) {

    for (ptr=listing[i].c_str(); ptr; ++ptr) {
      if (!isdigit(*ptr))
        break;
    }

    if (*ptr == 0 || (ptr > listing[i].c_str() && !strcmp(ptr, ".bad"))) {
      id = atoi(listing[i].c_str());
      if (id >= *nextidp)
        *nextidp = id+1;
    }
  
    if (*ptr != 0) {
      HT_WARNF("Invalid META LOG file name encountered '%s', skipping...",
               listing[i].c_str());
      continue;
    }

    file_ids.push_back(id);
  }

}
