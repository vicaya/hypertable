/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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

#include "Common/Compat.h"
#include <iostream>

#include "Common/Error.h"
#include "Common/Usage.h"

#include "CommandAttrSet.h"
#include "Global.h"
#include "Util.h"

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

const char *CommandAttrSet::ms_usage[] = {
  "attrset <file> <name>=<value> [i16|i32|i64]",
  "  This command issues a ATTRSET request to Hyperspace.",
  (const char *)0
};

void CommandAttrSet::run() {
  uint64_t handle;
  const char *value;

  if (m_args.size() < 2)
    HT_THROW(Error::PARSE_ERROR, "Wrong number of arguments.  Type 'help' for usage.");

  if (m_args[0].second != "")
    HT_THROWF(Error::PARSE_ERROR, "Invalid argument - %s", m_args[0].second.c_str());

  handle = Util::get_handle(m_args[0].first);

  value = m_args[1].second.c_str();

  if (m_args.size() == 3) {
    if (m_args[2].first == "i16") {
      uint16_t i16 = atoi(value);
      m_session->attr_set(handle, m_args[1].first, &i16, sizeof(i16));
    }
    else if (m_args[2].first == "i32") {
      uint32_t i32 = atoi(value);
      m_session->attr_set(handle, m_args[1].first, &i32, sizeof(i32));
    }
    else if (m_args[2].first == "i64") {
      uint64_t i64 = atoll(value);
      m_session->attr_set(handle, m_args[1].first, &i64, sizeof(i64));
    }
  }
  else
    m_session->attr_set(handle, m_args[1].first, value, strlen(value));
}
