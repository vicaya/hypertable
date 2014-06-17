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

#ifndef HYPERTABLE_METALOGENTITYHEADER_H
#define HYPERTABLE_METALOGENTITYHEADER_H

#include <iostream>

#include "Common/Mutex.h"

namespace Hypertable {

  namespace MetaLog {

    class EntityHeader {

    public:

      // enumerations
      enum {
        FLAG_REMOVE = 0x00000001,
        LENGTH = 32
      };

      EntityHeader();
      EntityHeader(int32_t type_);
      EntityHeader(const EntityHeader &other);
      bool operator<(const EntityHeader &other) const;

      void encode(uint8_t **bufp) const;
      void decode(const uint8_t **bufp, size_t *remainp);

      void display(std::ostream &os);

      int32_t type;
      int32_t checksum;
      int64_t id;
      int64_t timestamp;
      int32_t flags;
      int32_t length;

      static bool display_timestamp;

    private:
      static int64_t ms_next_id;
      static Mutex ms_mutex;
    };
  }
}

#endif // HYPERTABLE_METALOGENTITYHEADER_H
