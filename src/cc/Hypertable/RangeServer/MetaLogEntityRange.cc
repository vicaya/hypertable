/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
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

#include "MetaLogEntityRange.h"

using namespace Hypertable;
using namespace Hypertable::MetaLog;

EntityRange::EntityRange(int32_t type) : Entity(type) { }

EntityRange::EntityRange(const EntityHeader &header_) : Entity(header_) { }

EntityRange::EntityRange(const TableIdentifier &identifier,
                         const RangeSpec &range, const RangeState &state_) 
  : Entity(EntityType::RANGE), table(identifier), spec(range), state(state_) {
}


size_t EntityRange::encoded_length() const {
  return table.encoded_length() + spec.encoded_length() +
    state.encoded_length();
}


void EntityRange::encode(uint8_t **bufp) const {
  table.encode(bufp);
  spec.encode(bufp);
  state.encode(bufp);
}

void EntityRange::decode(const uint8_t **bufp, size_t *remainp) {
  table.decode(bufp, remainp);
  spec.decode(bufp, remainp);
  state.decode(bufp, remainp);
}

const String EntityRange::name() {
  return "Range";
}

void EntityRange::display(std::ostream &os) {
  os << " " << table << " " << spec << " " << state << " ";
}

