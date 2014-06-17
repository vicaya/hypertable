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
#include "Common/Serialization.h"

#include "MetaLogEntityOperation.h"

using namespace Hypertable;
using namespace Hypertable::MetaLog;


size_t EntityOperation::encoded_dependencies_length() const {
  size_t length = Serialization::encoded_length_vi32(m_dependent_servers.size()) +
    Serialization::encoded_length_vi32(m_dependent_tables.size());
  for (size_t i=0; i<m_dependent_servers.size(); i++)
    length += Serialization::encoded_length_vstr(m_dependent_servers[i]);
  for (size_t i=0; i<m_dependent_tables.size(); i++)
    length += Serialization::encoded_length_vstr(m_dependent_tables[i]);
  return length;
}

void EntityOperation::encode_dependencies(uint8_t **bufp) const {
  Serialization::encode_vi32(bufp, m_dependent_servers.size());
  for (size_t i=0; i<m_dependent_servers.size(); i++)
    Serialization::encode_vstr(bufp, m_dependent_servers[i]);
  Serialization::encode_vi32(bufp, m_dependent_tables.size());
  for (size_t i=0; i<m_dependent_tables.size(); i++)
    Serialization::encode_vstr(bufp, m_dependent_tables[i]);
}

void EntityOperation::decode_dependencies(const uint8_t **bufp, size_t *remainp) {
  String str;
  size_t length;

  m_dependent_servers.clear();
  length = Serialization::decode_vi32(bufp, remainp);
  for (size_t i=0; i<length; i++) {
    str = Serialization::decode_vstr(bufp, remainp);
    m_dependent_servers.push_back(str);
  }

  m_dependent_tables.clear();
  length = Serialization::decode_vi32(bufp, remainp);
  for (size_t i=0; i<length; i++) {
    str = Serialization::decode_vstr(bufp, remainp);
    m_dependent_tables.push_back(str);
  }
  
}

void EntityOperation::display_dependencies(std::ostream &os) {
  ScopedLock lock(m_mutex);
  bool first = true;

  os << " server-dependencies=";
  for (size_t i=0; i<m_dependent_servers.size(); i++) {
    if (first) {
      os << m_dependent_servers[i];
      first = false;
    }
    else
      os << m_dependent_servers[i] << ",";
  }

  first = true;
  os << " table-dependencies=";
  for (size_t i=0; i<m_dependent_tables.size(); i++) {
    if (first) {
      os << m_dependent_tables[i];
      first = false;
    }
    else
      os << m_dependent_tables[i] << ",";
  }
  os << " ";
  
}
