/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
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

#ifndef HYPERTABLE_CRASHTEST_H
#define HYPERTABLE_CRASHTEST_H

#include "HashMap.h"
#include "String.h"
#include "StringExt.h"

namespace Hypertable {

  class CrashTest {
  public:
    void parse_option(String option);
    void maybe_crash(const String &label);
  private:
    typedef hash_map<String, uint32_t> CountDownMap;
    CountDownMap m_countdown_map;
  };

}

#endif // HYPERTABLE_CRASHTEST_H
