/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include <cstring>
#include <iostream>

#include "AsyncComm/CommBuf.h"
#include "Common/Serialization.h"

#include "Types.h"

using namespace Hypertable;
using namespace std;

const uint64_t Hypertable::END_OF_TIME = (uint64_t)-1;

size_t TableIdentifier::encoded_length() {
  return 8 + Serialization::encoded_length_string(name);
}

void TableIdentifier::encode(uint8_t **buf_ptr) {
  Serialization::encode_string(buf_ptr, name);
  Serialization::encode_int(buf_ptr, id);
  Serialization::encode_int(buf_ptr, generation);
}

bool TableIdentifier::decode(uint8_t **buf_ptr, size_t *remainingPtr) {
  name = 0;
  id = generation = 0;
  if (!Serialization::decode_string(buf_ptr, remainingPtr, &name))
    return false;
  if (!Serialization::decode_int(buf_ptr, remainingPtr, &id))
    return false;
  if (!Serialization::decode_int(buf_ptr, remainingPtr, &generation))
    return false;
  return true;
}

size_t RangeSpec::encoded_length() {
  return Serialization::encoded_length_string(start_row) + Serialization::encoded_length_string(end_row);
}

void RangeSpec::encode(uint8_t **buf_ptr) {
  Serialization::encode_string(buf_ptr, start_row);
  Serialization::encode_string(buf_ptr, end_row);
}

bool RangeSpec::decode(uint8_t **buf_ptr, size_t *remainingPtr) {
  start_row = end_row = 0;
  if (!Serialization::decode_string(buf_ptr, remainingPtr, &start_row))
    return false;
  if (!Serialization::decode_string(buf_ptr, remainingPtr, &end_row))
    return false;
  return true;
}


ScanSpec::ScanSpec() : row_limit(0), max_versions(0), start_row(0), start_row_inclusive(true), end_row(0), end_row_inclusive(true), interval(0, END_OF_TIME), return_deletes(false) {
  return;
}

size_t ScanSpec::encoded_length() {
  size_t len = 29;
  len += Serialization::encoded_length_string(start_row);
  len += Serialization::encoded_length_string(end_row);
  for (size_t i=0; i<columns.size(); i++)
    len += Serialization::encoded_length_string(columns[i]);
  return len;
}

void ScanSpec::encode(uint8_t **buf_ptr) {
  Serialization::encode_int(buf_ptr, row_limit);
  Serialization::encode_int(buf_ptr, max_versions);
  Serialization::encode_string(buf_ptr, start_row);
  *(*buf_ptr)++ = (uint8_t)start_row_inclusive;
  Serialization::encode_string(buf_ptr, end_row);
  *(*buf_ptr)++ = (uint8_t)end_row_inclusive;
  Serialization::encode_short(buf_ptr, (short)columns.size());
  for (size_t i=0; i<columns.size(); i++)
    Serialization::encode_string(buf_ptr, columns[i]);
  Serialization::encode_long(buf_ptr, interval.first);
  Serialization::encode_long(buf_ptr, interval.second);
  Serialization::encode_bool(buf_ptr, return_deletes);
}

bool ScanSpec::decode(uint8_t **buf_ptr, size_t *remainingPtr) {
  uint16_t columnCount;
  const char *column;
  uint8_t inclusiveByte;

  if (!Serialization::decode_int(buf_ptr, remainingPtr, &row_limit))
    return false;
  if (!Serialization::decode_int(buf_ptr, remainingPtr, &max_versions))
    return false;
  if (!Serialization::decode_string(buf_ptr, remainingPtr, &start_row))
    return false;
  if (!Serialization::decode_byte(buf_ptr, remainingPtr, &inclusiveByte))
    return false;
  start_row_inclusive = inclusiveByte ? true : false;
  if (!Serialization::decode_string(buf_ptr, remainingPtr, &end_row))
    return false;
  if (!Serialization::decode_byte(buf_ptr, remainingPtr, &inclusiveByte))
    return false;
  end_row_inclusive = inclusiveByte ? true : false;
  if (!Serialization::decode_short(buf_ptr, remainingPtr, &columnCount))
    return false;
  for (short i=0; i<columnCount; i++) {
    if (!Serialization::decode_string(buf_ptr, remainingPtr, &column))
      return false;
    columns.push_back(column);
  }
  if (!Serialization::decode_long(buf_ptr, remainingPtr, &interval.first))
    return false;
  if (!Serialization::decode_long(buf_ptr, remainingPtr, &interval.second))
    return false;
  if (!Serialization::decode_bool(buf_ptr, remainingPtr, &return_deletes))
    return false;
  
  return true;
}


std::ostream &Hypertable::operator<<(std::ostream &os, const TableIdentifier &table_identifier) {
  os << "Table Name = " << table_identifier.name << endl;
  os << "Table ID   = " << table_identifier.id << endl;
  os << "Generation = " << table_identifier.generation << endl;
  return os;
}

std::ostream &Hypertable::operator<<(std::ostream &os, const RangeSpec &range) {
  if (range.start_row == 0)
    os << "StartRow = [NULL]" << endl;
  else
    os << "StartRow = \"" << range.start_row << "\"" << endl;
  if (range.end_row == 0)
    os << "EndRow   = [NULL]" << endl;
  else
    os << "EndRow   = \"" << range.end_row << "\"" << endl;
  return os;
}

std::ostream &Hypertable::operator<<(std::ostream &os, const ScanSpec &scanSpec) {
  os << "RowLimit    = " << scanSpec.row_limit << endl;
  os << "MaxVersions = " << scanSpec.max_versions << endl;
  os << "Columns     = ";
  for (std::vector<const char *>::const_iterator iter = scanSpec.columns.begin(); iter != scanSpec.columns.end(); iter++)
    os << *iter << " ";
  os << endl;
  if (scanSpec.start_row)
    os << "StartRow  = " << scanSpec.start_row << endl;
  else
    os << "StartRow  = " << endl;
  os << "StartRowInclusive = " << scanSpec.start_row_inclusive << endl;
  if (scanSpec.end_row)
    os << "EndRow    = " << scanSpec.end_row << endl;
  else
    os << "EndRow    = " << endl;
  os << "EndRowInclusive = " << scanSpec.end_row_inclusive << endl;
  os << "MinTime     = " << scanSpec.interval.first << endl;
  os << "MaxTime     = " << scanSpec.interval.second << endl;
  return os;
}


