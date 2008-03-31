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

#ifndef HYPERTABLE_METALOG_READER_H
#define HYPERTABLE_METALOG_READER_H

#include "MetaLog.h"

namespace Hypertable {

namespace MetaLogEntryFactory {
  MetaLogEntry *new_from_payload(int type, const void *buf, size_t len);
}

class MetaLogReader {
public:
  struct ScanEntry {
    int type;
    uint32_t checksum;
    size_t payload_size;
    const void *payload;
  };

public:
  virtual ~MetaLogReader() {}
  
  // quick scan without deserialize entries, throws if invalid
  virtual ScanEntry *next(ScanEntry *) = 0;
  virtual ScanEntry *last(ScanEntry *) = 0;

  // read and get ready for the next record, throws if invalid
  virtual MetaLogEntry *read() {
    // depends on how smart the compiler is, this maybe inlined
    ScanEntry se;

    if (!next(&se))
      return NULL;

    return MetaLogEntryFactory::new_from_payload(se.type, se.payload,
                                                 se.payload_size);
  }

  virtual MetaLogEntry *read_last() {
    ScanEntry se;

    if (!last(&se))
      return NULL;

    return MetaLogEntryFactory::new_from_payload(se.type, se.payload,
                                                 se.payload_size);
  }
};

} // namespace Hypertable

#endif // HYPERTABLE_METALOG_READER_H
