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

#ifndef HYPERTABLE_KEYSPEC_H
#define HYPERTABLE_KEYSPEC_H

#include <boost/noncopyable.hpp>

#include <vector>
#include <limits>

#include "Common/String.h"

namespace Hypertable {

  const int64_t TIMESTAMP_MIN  = INT64_MIN;
  const int64_t TIMESTAMP_MAX  = INT64_MAX;
  const int64_t TIMESTAMP_NULL = INT64_MIN + 1;
  const int64_t TIMESTAMP_AUTO = INT64_MIN + 2;
  const int64_t AUTO_ASSIGN    = INT64_MIN + 2;

  class KeySpec {
  public:

    KeySpec() : row(0), row_len(0), column_family(0), column_qualifier(0),
                column_qualifier_len(0), timestamp(AUTO_ASSIGN),
                revision(AUTO_ASSIGN) {}

    explicit KeySpec(const char *r, const char *cf, const char *cq,
                     int64_t ts = AUTO_ASSIGN)
      : row(r), row_len(r ? strlen(r) : 0), column_family(cf),
        column_qualifier(cq), column_qualifier_len(cq ? strlen(cq) : 0),
        timestamp(ts), revision(AUTO_ASSIGN) {}

    explicit KeySpec(const char *r, const char *cf,
                     int64_t ts = AUTO_ASSIGN)
      : row(r), row_len(r ? strlen(r) : 0), column_family(cf),
        column_qualifier(0), column_qualifier_len(0),
        timestamp(ts), revision(AUTO_ASSIGN) {}

    void clear() {
      row = 0;
      row_len = 0;
      column_family = 0;
      column_qualifier = 0;
      column_qualifier_len = 0;
      timestamp = AUTO_ASSIGN;
      revision = AUTO_ASSIGN;
    }

    const void  *row;
    size_t       row_len;
    const char  *column_family;
    const void  *column_qualifier;
    size_t       column_qualifier_len;
    int64_t      timestamp;
    int64_t      revision;  // internal use only
  };


  /**
   * Helper class for building a KeySpec.  This class manages the allocation
   * of all string members.
   */
  class KeySpecBuilder : boost::noncopyable {
  public:
    void set_row(const String &row) {
      m_strings.push_back(row);
      m_key_spec.row = m_strings.back().c_str();
      m_key_spec.row_len = m_strings.back().length();
    }

    void set_column_family(const String &cf) {
      m_strings.push_back(cf);
      m_key_spec.column_family = m_strings.back().c_str();
    }

    void set_column_qualifier(const String &cq) {
      m_strings.push_back(cq);
      m_key_spec.column_qualifier = m_strings.back().c_str();
      m_key_spec.column_qualifier_len = m_strings.back().length();
    }

    void set_timestamp(int64_t timestamp) {
      m_key_spec.timestamp = timestamp;
    }

    /**
     * Clears the state.
     */
    void clear() {
      m_key_spec.clear();
      m_strings.clear();
    }

    /**
     * Returns the built KeySpec object
     *
     * @return reference to built KeySpec object
     */
    KeySpec &get() { return m_key_spec; }

  private:
    std::vector<String> m_strings;
    KeySpec m_key_spec;
  };


}

#endif // HYPERTABLE_KEYSPEC_H

