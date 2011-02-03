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

#ifndef HYPERTABLE_METALOGENTITYRANGE_H
#define HYPERTABLE_METALOGENTITYRANGE_H

#include "Hypertable/Lib/MetaLogEntity.h"
#include "Hypertable/Lib/RangeState.h"
#include "Hypertable/Lib/Types.h"

namespace Hypertable {
  namespace MetaLog {

    class EntityRange : public Entity {
    public:
      EntityRange(int32_t type);
      EntityRange(const EntityHeader &header_);
      EntityRange(const TableIdentifier &identifier,
                  const RangeSpec &range, const RangeState &state_);
      virtual ~EntityRange() { }
      virtual size_t encoded_length() const;
      virtual void encode(uint8_t **bufp) const;
      virtual void decode(const uint8_t **bufp, size_t *remainp);
      virtual const String name();
      virtual void display(std::ostream &os);

      TableIdentifierManaged table;
      RangeSpecManaged spec;
      RangeStateManaged state;
    };
    typedef intrusive_ptr<EntityRange> EntityRangePtr;

    namespace EntityType {
      enum {
        RANGE = 0x00010001
      };
    }
    

  } // namespace MetaLog
} // namespace Hypertable

#endif // HYPERTABLE_METALOGENTITYRANGE_H
