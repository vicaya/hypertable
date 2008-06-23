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

#ifndef HYPERTABLE_KEYSPEC_H
#define HYPERTABLE_KEYSPEC_H

#include "Common/String.h"

namespace Hypertable {

  class KeySpec {
  public:
    KeySpec() : row(0), row_len(0), column_family(0), column_qualifier(0),
        column_qualifier_len(0) {}

    explicit KeySpec(const char *r, const char *cf, const char *cq = 0)
        : row(r), row_len(r ? strlen(r) : 0), column_family(cf),
          column_qualifier(cq), column_qualifier_len(cq ? strlen(cq) : 0) {}

    explicit KeySpec(const String &r, const String &cf = String(),
                     const String & cq = String())
        : row(r.size() ? r.c_str() : 0), row_len(r.length()),
          column_family(cf.size() ? cf.c_str() : 0),
          column_qualifier(cq.size() ? cq.c_str() : 0),
          column_qualifier_len(cq.length()) {}

    const void  *row;
    size_t       row_len;
    const char  *column_family;
    const void  *column_qualifier;
    size_t       column_qualifier_len;
  };

}

#endif // HYPERTABLE_KEYSPEC_H

