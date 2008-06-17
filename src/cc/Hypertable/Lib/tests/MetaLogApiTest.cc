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

#include "Common/Compat.h"
#include "Hypertable/Lib/MetaLogReader.h"
#include <boost/scoped_ptr.hpp>

using namespace Hypertable;

namespace MetaLogEntryFactory {

MetaLogEntry *
new_from_payload(int type, const void *buf, size_t len) {
  return NULL;
}

} // MetaLogEntryFactory

using namespace MetaLogEntryFactory;

namespace {

struct NullEntry : public MetaLogEntry {
  virtual void write(DynamicBuffer &)  {}
  virtual void read(const void *buf, size_t len) {}
  virtual bool is_valid() { return true; }
  virtual int get_type() { return 0; }
};

struct NullLog : public MetaLog {
  virtual void write(MetaLogEntry *) {}
  virtual void close() {}
  virtual void purge() {}
};

struct NullReader : public MetaLogReader {
  virtual ScanEntry *next(ScanEntry *) { return NULL; }
  virtual MetaLogEntry *read() { return NULL; }
};

void
null_test() {
  MetaLogPtr metalog(new NullLog);
  MetaLogReaderPtr reader(new NullReader);
  MetaLogEntryPtr log_entry(new NullEntry);

  metalog->write(log_entry.get());
  metalog->purge();
  metalog->close();

  reader->read();
}

} // local namespace

int
main() {
  null_test();
  return 0;
}
