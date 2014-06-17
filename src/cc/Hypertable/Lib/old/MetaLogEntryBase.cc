/**
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
#include "MetaLogEntryBase.h"

using namespace Hypertable;
using namespace Hypertable::OldMetaLog;

MetaLogEntryRangeBase::MetaLogEntryRangeBase(const TableIdentifier &t,
                                             const RangeSpec &r)
    : table(t), range(r) {
}

void
MetaLogEntryRangeBase::write(DynamicBuffer &buf) {
  size_t elen = table.encoded_length() + range.encoded_length();
  buf.ensure(elen);
  table.encode(&buf.ptr);
  range.encode(&buf.ptr);
}

const uint8_t *
MetaLogEntryRangeBase::read(StaticBuffer &in) {
  TableIdentifier tmp_table;
  RangeSpec tmp_range;
  buffer = in;
  const uint8_t *p = buffer.base;
  size_t remain = buffer.size;
  HT_TRY("decoding range base",
         tmp_table.decode(&p, &remain);
         tmp_range.decode(&p, &remain));
  table = tmp_table;
  range = tmp_range;
  return p;
}


void
MetaLogEntryRangeCommon::write(DynamicBuffer &buf) {
  Parent::write(buf);
  buf.ensure(range_state.encoded_length());
  range_state.encode(&buf.ptr);
}

const uint8_t *
MetaLogEntryRangeCommon::read(StaticBuffer &in) {
  HT_TRY("decoding range base",
    const uint8_t *p = Parent::read(in);
    size_t remain = buffer_remain(p);
    range_state.decode(&p, &remain);
    return p);
}
