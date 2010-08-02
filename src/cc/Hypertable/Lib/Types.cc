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

const char *TableIdentifier::METADATA_ID = "0";

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

RangeIdentifier::RangeIdentifier(const char *start_row, const char *end_row) {
  set(start_row, end_row);
}

bool RangeIdentifier::operator==(const RangeIdentifier &other) const {
  return (0==memcmp((const void *)id, (const void *)other.id, length));
}

bool RangeIdentifier::operator<(const RangeIdentifier &other) const {
  return (0 > memcmp((const void *)id, (const void *)other.id, length));
}

void RangeIdentifier::set(const char* start_row, const char *end_row) {
  md5_context ctx;
  memset(&ctx, 0, sizeof(ctx));
  md5_starts(&ctx);
  md5_update(&ctx, (const unsigned char *)start_row, strlen(start_row));
  md5_update(&ctx, (const unsigned char *)end_row, strlen(end_row));
  md5_finish(&ctx, id);
}

void RangeIdentifier::encode(uint8_t **bufp) const {
  memcpy((void*)*bufp, (void*)id, length);
  *bufp += length;
}

void RangeIdentifier::decode(const uint8_t **bufp, size_t *remainp) {
  memcpy((void *)id, (void*)*bufp, length);
  *bufp += length;
  *remainp -= length;
}

void RangeIdentifier::to_str(String &out) const {
  static const char hex[] = {'0', '1', '2', '3', '4', '5', '6', '7','8', '9', 'A',
                             'B', 'C', 'D', 'E', 'F'};
  char str[33];
  int hex_digit;
  for (uint32_t ii=0; ii<length; ++ii) {
    hex_digit = id[ii] >> 4; // top 4 bits
    str[ii*2] = hex[hex_digit];
    hex_digit = id[ii] & 0x0f; // bottom 4 bits
    str[ii*2+1]= hex[hex_digit];
  }
  str[32] = '\0';
  out = str;
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

ostream &Hypertable::operator<<(ostream &os, const RangeIdentifier &range_id) {
  String str;
  range_id.to_str(str);
  os <<"{RangeId:" << str << "}";
  return os;
}


