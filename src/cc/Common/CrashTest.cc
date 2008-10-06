/**
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

#include "Compat.h"
#include "CrashTest.h"
#include "Error.h"
#include "Logger.h"

using namespace Hypertable;

void CrashTest::parse_option(String option) {
  char *istr = strchr(option.c_str(), ':');
  HT_ASSERT(istr != 0);
  *istr++ = 0;
  m_countdown_map[option.c_str()] = atoi(istr);
}

void CrashTest::maybe_crash(const String &label) {
  CountDownMap::iterator iter = m_countdown_map.find(label);
  if (iter != m_countdown_map.end()) {
    if ((*iter).second == 0)
      _exit(1);
    m_countdown_map[label] = (*iter).second - 1;
  }
}
