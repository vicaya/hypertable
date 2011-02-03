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

#ifndef HYPERTABLE_METALOG_H
#define HYPERTABLE_METALOG_H

#include <iostream>
#include <vector>

#include "Common/Filesystem.h"
#include "Common/Mutex.h"
#include "Common/ReferenceCount.h"

namespace Hypertable {

  namespace MetaLog {
    class Header {
    public:
      void encode(uint8_t **bufp) const;
      void decode(const uint8_t **bufp, size_t *remainp);

      enum {
        LENGTH = 16
      };

      uint16_t version;
      char name[14];
    };
    void scan_log_directory(FilesystemPtr &fs, const String &path,
                            std::vector<int32_t> &file_ids, int32_t *nextidp);
  }
}

#endif // HYPERTABLE_METALOG_H
