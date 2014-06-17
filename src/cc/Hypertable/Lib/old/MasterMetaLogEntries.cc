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
#include "Common/Logger.h"
#include "Common/Serialization.h"
#include "Hypertable/Lib/Types.h"

#include "MasterMetaLogEntries.h"


using namespace Hypertable;
using namespace Hypertable::OldMetaLog;
using namespace Hypertable::OldMetaLog::MasterTxn;
using namespace Serialization;

void
ServerJoined::write(DynamicBuffer &buf) {
  buf.ensure(Serialization::encoded_length_vstr(location));
  Serialization::encode_vstr(&buf.ptr, location);
}

const uint8_t *
ServerJoined::read(StaticBuffer &in) {
  buffer = in;
  const uint8_t *p = buffer.base;
  size_t remain = buffer.size;
  HT_TRY("decoding server joined",
         location = Serialization::decode_vstr(&p, &remain));
  return p;
}

void
ServerLeft::write(DynamicBuffer &buf) {
  buf.ensure(Serialization::encoded_length_vstr(location));
  Serialization::encode_vstr(&buf.ptr, location);
}

const uint8_t *
ServerLeft::read(StaticBuffer &in) {
  buffer = in;
  const uint8_t *p = buffer.base;
  size_t remain = buffer.size;
  HT_TRY("decoding server left",
         location = Serialization::decode_vstr(&p, &remain));
  return p;
}

void
ServerRemoved::write(DynamicBuffer &buf) {
  buf.ensure(Serialization::encoded_length_vstr(location));
  Serialization::encode_vstr(&buf.ptr, location);
}

const uint8_t *
ServerRemoved::read(StaticBuffer &in) {
  buffer = in;
  const uint8_t *p = buffer.base;
  size_t remain = buffer.size;
  HT_TRY("decoding server removed",
         location = Serialization::decode_vstr(&p, &remain));
  return p;
}

void
RangeMoveStarted::write(DynamicBuffer &buf) {
  buf.ensure(table.encoded_length() +
             range.encoded_length() +
             Serialization::encoded_length_vstr(transfer_log) +
             8 +
             Serialization::encoded_length_vstr(location));
  table.encode(&buf.ptr);
  range.encode(&buf.ptr);
  Serialization::encode_vstr(&buf.ptr, transfer_log);
  Serialization::encode_i64(&buf.ptr, soft_limit);
  Serialization::encode_vstr(&buf.ptr, location);
}

const uint8_t *
RangeMoveStarted::read(StaticBuffer &in) {
  TableIdentifier tmp_table;
  RangeSpec tmp_range;
  buffer = in;
  const uint8_t *p = buffer.base;
  size_t remain = buffer.size;
  HT_TRY("decoding range move started",
         tmp_table.decode(&p, &remain);
         tmp_range.decode(&p, &remain);
         transfer_log = Serialization::decode_vstr(&p, &remain);
         soft_limit = Serialization::decode_i64(&p, &remain);
         location = Serialization::decode_vstr(&p, &remain));
  table = tmp_table;
  range = tmp_range;
  return p;
}

void
RangeMoveRestarted::write(DynamicBuffer &buf) {
  buf.ensure(table.encoded_length() +
             range.encoded_length() +
             Serialization::encoded_length_vstr(transfer_log) +
             8 +
             Serialization::encoded_length_vstr(location));
  table.encode(&buf.ptr);
  range.encode(&buf.ptr);
  Serialization::encode_vstr(&buf.ptr, transfer_log);
  Serialization::encode_i64(&buf.ptr, soft_limit);
  Serialization::encode_vstr(&buf.ptr, location);
}

const uint8_t *
RangeMoveRestarted::read(StaticBuffer &in) {
  TableIdentifier tmp_table;
  RangeSpec tmp_range;
  buffer = in;
  const uint8_t *p = buffer.base;
  size_t remain = buffer.size;
  HT_TRY("decoding range move restarted",
         tmp_table.decode(&p, &remain);
         tmp_range.decode(&p, &remain);
         transfer_log = Serialization::decode_vstr(&p, &remain);
         soft_limit = Serialization::decode_i64(&p, &remain);
         location = Serialization::decode_vstr(&p, &remain));
  table = tmp_table;
  range = tmp_range;
  return p;
}

void
RangeMoveLoaded::write(DynamicBuffer &buf) {
  buf.ensure(table.encoded_length() + range.encoded_length() +
             Serialization::encoded_length_vstr(location));
  table.encode(&buf.ptr);
  range.encode(&buf.ptr);
  Serialization::encode_vstr(&buf.ptr, location);
}

const uint8_t *
RangeMoveLoaded::read(StaticBuffer &in) {
  TableIdentifier tmp_table;
  RangeSpec tmp_range;
  buffer = in;
  const uint8_t *p = buffer.base;
  size_t remain = buffer.size;
  HT_TRY("decoding range move loaded",
         tmp_table.decode(&p, &remain);
         tmp_range.decode(&p, &remain);
         location = Serialization::decode_vstr(&p, &remain));
  table = tmp_table;
  range = tmp_range;
  return p;
}

void
RangeMoveAcknowledged::write(DynamicBuffer &buf) {
  buf.ensure(table.encoded_length() + range.encoded_length() +
             Serialization::encoded_length_vstr(location));
  table.encode(&buf.ptr);
  range.encode(&buf.ptr);
  Serialization::encode_vstr(&buf.ptr, location);
}

const uint8_t *
RangeMoveAcknowledged::read(StaticBuffer &in) {
  TableIdentifier tmp_table;
  RangeSpec tmp_range;
  buffer = in;
  const uint8_t *p = buffer.base;
  size_t remain = buffer.size;
  HT_TRY("decoding range move acknowledged",
         tmp_table.decode(&p, &remain);
         tmp_range.decode(&p, &remain);
         location = Serialization::decode_vstr(&p, &remain));
  table = tmp_table;
  range = tmp_range;
  return p;
}
