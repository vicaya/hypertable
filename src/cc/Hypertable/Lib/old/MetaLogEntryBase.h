/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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

#ifndef HYPERTABLE_METALOG_ENTRY_BASE_H
#define HYPERTABLE_METALOG_ENTRY_BASE_H

#include "Common/Logger.h"

#include "Hypertable/Lib/Types.h"
#include "Hypertable/Lib/RangeState.h"

#include "MetaLog.h"

namespace Hypertable {
  namespace OldMetaLog {

    /** base impl classes for various entries */

    class MetaLogEntryRangeBase : public MetaLogEntry {
    public:
      MetaLogEntryRangeBase() {}
      MetaLogEntryRangeBase(const TableIdentifier &, const RangeSpec &);

      virtual void write(DynamicBuffer &);
      virtual const uint8_t *read(StaticBuffer &);

      /** Helper method for serialization
       *
       * @param p - a pointer between buffer.base and buffer.base + buffer.size
       * @return remaining space in bytes (base + size - p)
       * @throw input overrun exception if p violate precondition.
       */
      size_t buffer_remain(const uint8_t *p) {
        if (p < buffer.base && p > buffer.base + buffer.size)
          HT_THROW(Error::METALOG_ENTRY_TRUNCATED, "checking buffer space");
        return buffer.base + buffer.size - p;
      }

      TableIdentifierManaged table;
      RangeSpecManaged range;
    };

    class MetaLogEntryRangeCommon: public MetaLogEntryRangeBase {
    public:
      typedef MetaLogEntryRangeBase Parent;

      MetaLogEntryRangeCommon() {}
      MetaLogEntryRangeCommon(const TableIdentifier &t, const RangeSpec &r,
                              const RangeState &state)
        : MetaLogEntryRangeBase(t, r), range_state(state) {}

      virtual void write(DynamicBuffer &);
      virtual const uint8_t *read(StaticBuffer &);

      RangeState range_state;
    };

  } // namespace OldMetaLog

} // namespace Hypertable

#endif // HYPERTABLE_METALOG_ENTRY_BASE_H
