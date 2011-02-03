/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include "Common/Serialization.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"
extern "C" {
#include "Common/md5.h"
}
#include "Types.h"

using namespace std;
using namespace Hypertable;
using namespace Serialization;

const char *TableIdentifier::METADATA_ID = "0/0";
const char *TableIdentifier::METADATA_NAME= "sys/METADATA";
const int TableIdentifier::METADATA_ID_LENGTH = 3;

bool TableIdentifier::operator==(const TableIdentifier &other) const {
  if (strcmp(id, other.id) ||
      generation != other.generation)
    return false;
  return true;
}

bool TableIdentifier::operator!=(const TableIdentifier &other) const {
  return !(*this == other);
}


size_t TableIdentifier::encoded_length() const {
  return 4 + encoded_length_vstr(id);
}

void TableIdentifier::encode(uint8_t **bufp) const {
  encode_vstr(bufp, id);
  encode_i32(bufp, generation);
}

void TableIdentifier::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding table identitier",
    id = decode_vstr(bufp, remainp);
    generation = decode_i32(bufp, remainp));
}

void TableIdentifierManaged::decode(const uint8_t **bufp, size_t *remainp) {
  TableIdentifier::decode(bufp, remainp);
  *this = *this;
}

bool RangeSpec::operator==(const RangeSpec &other) const {
  if (start_row == 0 || other.start_row == 0) {
    if (start_row != other.start_row)
      return false;
  }
  else {
    if (strcmp(start_row, other.start_row))
      return false;
  }
  if (end_row == 0 || other.end_row == 0) {
    if (end_row != other.end_row)
      return false;
  }
  else {
    if (strcmp(end_row, other.end_row))
      return false;
  }
  return true;
}

bool RangeSpec::operator!=(const RangeSpec &other) const {
  return !(*this == other);
}

size_t RangeSpec::encoded_length() const {
  return encoded_length_vstr(start_row) + encoded_length_vstr(end_row);
}

void RangeSpec::encode(uint8_t **bufp) const {
  encode_vstr(bufp, start_row);
  encode_vstr(bufp, end_row);
}

void RangeSpec::decode(const uint8_t **bufp, size_t *remainp) {
  HT_TRY("decoding range spec",
    start_row = decode_vstr(bufp, remainp);
    end_row = decode_vstr(bufp, remainp));
}

void RangeSpecManaged::decode(const uint8_t **bufp, size_t *remainp) {
  RangeSpec::decode(bufp, remainp);
  *this = *this;
}

ostream &Hypertable::operator<<(ostream &os, const TableIdentifier &tid) {
  os <<"{TableIdentifier: id='"<< tid.id
     <<"' generation="<< tid.generation <<"}";
  return os;
}

ostream &Hypertable::operator<<(ostream &os, const RangeSpec &range) {
  os <<"{RangeSpec:";

  HT_DUMP_CSTR(os, start, range.start_row);
  HT_DUMP_CSTR(os, end, range.end_row);

  os <<'}';
  return os;
}


