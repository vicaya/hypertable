/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 3 of the
 * License.
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

#ifndef HYPERTABLE_STATSSERIALIZABLE_H
#define HYPERTABLE_STATSSERIALIZABLE_H

extern "C" {
#include <stddef.h>
#include <stdint.h>
}

#include "ReferenceCount.h"

namespace Hypertable {

  class StatsSerializable : public ReferenceCount {
  public:
    StatsSerializable(uint16_t _id, uint8_t _group_count);
    StatsSerializable(const StatsSerializable &other);
    StatsSerializable &operator=(const StatsSerializable &other);
    size_t encoded_length() const;
    void encode(uint8_t **bufp) const;
    void decode(const uint8_t **bufp, size_t *remainp);
    bool operator==(const StatsSerializable &other) const;
    bool operator!=(const StatsSerializable &other) const {
      return !(*this == other);
    }


  protected:

    enum Identifier {
      SYSTEM = 1,
      RANGE = 2,
      TABLE = 3,
      RANGE_SERVER = 4
    };
    virtual size_t encoded_length_group(int group) const = 0;
    virtual void encode_group(int group, uint8_t **bufp) const = 0;
    virtual void decode_group(int group, uint16_t len, const uint8_t **bufp, size_t *remainp) = 0;

    uint16_t id;
    uint8_t group_count;
    uint8_t group_ids[32];
  };
}


#endif // HYPERTABLE_STATSSERIALIZABLE_H


