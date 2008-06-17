/** -*- c++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
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

#ifndef HYPERTABLE_METALOG_READER_H
#define HYPERTABLE_METALOG_READER_H

#include "MetaLog.h"
#include <vector>

namespace Hypertable {

/**
 * Interface for meta log readers
 */
class MetaLogReader : public ReferenceCount {
public:
  struct ScanEntry {
    int type;
    uint32_t checksum;
    size_t payload_size;
    const void *payload;
  };

public:
  virtual ~MetaLogReader() {}

  /**
   * quick scan without deserialize entries, throws if invalid
   */
  virtual ScanEntry *next(ScanEntry *) = 0;

  /**
   * read and get ready for the next record, throws if invalid
   */
  virtual MetaLogEntry *read() = 0;
};

typedef std::vector<MetaLogEntryPtr> MetaLogEntries;
typedef intrusive_ptr<MetaLogReader> MetaLogReaderPtr;

} // namespace Hypertable

#endif // HYPERTABLE_METALOG_READER_H
