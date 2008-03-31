/**
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_METALOG_H
#define HYPERTABLE_METALOG_H

namespace Hypertable {
/**
 * Abstract classes/interfaces for meta log classes
 * cf. http://code.google.com/p/hypertable/wiki/MetaLogDesignNotes
 */

class DynamicBuffer;

class MetaLogEntry {
public:
  virtual ~MetaLogEntry() {}

  virtual void write(DynamicBuffer &) = 0;
  virtual void read(const void *buf, size_t len) = 0;
  virtual bool is_valid() = 0;

  virtual int get_type() = 0;
};

class MetaLog {
public:
  virtual ~MetaLog() {}

  virtual void write(MetaLogEntry *) = 0;
  virtual void close() = 0;

  // Remove finished entries except rs_range_loaded
  virtual void compact() = 0;
};

} // namespace Hypertable

#endif // HYPERTABLE_METALOG_H
