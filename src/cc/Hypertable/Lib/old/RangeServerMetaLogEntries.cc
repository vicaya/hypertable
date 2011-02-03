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

#include "Common/Compat.h"

#include "Hypertable/Lib/Types.h"

#include "RangeServerMetaLogEntries.h"

using namespace Hypertable;
using namespace Hypertable::OldMetaLog;
using namespace Hypertable::OldMetaLog::RangeServerTxn;
using namespace Serialization;

void
DropTable::write(DynamicBuffer &buf) {
  buf.ensure(table.encoded_length());
  table.encode(&buf.ptr);
}

const uint8_t *
DropTable::read(StaticBuffer &in) {
  TableIdentifier tmp_table;
  buffer = in;
  const uint8_t *p = buffer.base;
  size_t remain = buffer.size;
  HT_TRY("decoding drop table",
         tmp_table.decode(&p, &remain));
  table = tmp_table;
  return p;
}
