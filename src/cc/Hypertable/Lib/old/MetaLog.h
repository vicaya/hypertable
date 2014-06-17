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

#ifndef HYPERTABLE_METALOG_H
#define HYPERTABLE_METALOG_H

#include "Common/ReferenceCount.h"
#include "Common/DynamicBuffer.h"
#include "Common/StaticBuffer.h"
#include "Common/Time.h"

/**
 * Abstract classes/interfaces for meta log classes
 * cf. http://code.google.com/p/hypertable/wiki/MetaLogDesignNotes
 */

namespace Hypertable {
  namespace OldMetaLog {

    class MetaLogEntry : public ReferenceCount {
    public:
      MetaLogEntry() : timestamp(get_ts64()) {}
      virtual ~MetaLogEntry() {}

      /**
       * Serialize to buffer
       *
       * @param buf - output buffer
       */
      virtual void write(DynamicBuffer &buf) = 0;

      /**
       * Deserialize from buffer
       *
       * @param buf - input buffer (consumed)
       * @return current pointer of input buffer for further reading
       */
      virtual const uint8_t *read(StaticBuffer &buf) = 0;

      /**
       * Get the most derived type of the entry
       *
       * @return type of entry
       */
      virtual int get_type() const = 0;

      int64_t timestamp;
      StaticBuffer buffer;
    };

    typedef intrusive_ptr<MetaLogEntry> MetaLogEntryPtr;

    std::ostream &operator<<(std::ostream &, const MetaLogEntry *);

    class MetaLog : public ReferenceCount {
    public:
      virtual ~MetaLog() {}

      /**
       * Write a metalog entry
       *
       * @param mle - metalog entry to write
       */
      virtual void write(MetaLogEntry *mle) = 0;

      /**
       * Close the metalog
       * Implementation should handle multiple close calls
       */
      virtual void close() = 0;
    };

    typedef intrusive_ptr<MetaLog> MetaLogPtr;

  } // namespace OldMetaLog
} // namespace Hypertable

#endif // HYPERTABLE_METALOG_H
