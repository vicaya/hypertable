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

#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Usage.h"

#include "CommandAttrGet.h"
#include "Global.h"
#include "Util.h"

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

const char *CommandAttrGet::ms_usage[] = {
  "attrget <file> <name> [int|short|long]",
  "  This command issues a ATTRGET request to Hyperspace.",
  (const char *)0
};


void CommandAttrGet::run() {
  uint64_t handle;
  DynamicBuffer value(0);

  if (m_args.size() < 2 || m_args.size() > 3)
    HT_THROW(Error::PARSE_ERROR, "Wrong number of arguments.  Type 'help' for usage.");

  if (m_args[0].second != "" || m_args[1].second != "")
    HT_THROW(Error::PARSE_ERROR, "Invalid character '=' in argument.");

  handle = Util::get_handle(m_args[0].first);

  m_session->attr_get(handle, m_args[1].first, value);

  if (m_args.size() == 3) {
    if (m_args[2].first == "short") {
      if (value.fill() != 2)
	HT_THROWF(Error::HYPERSPACE_BAD_ATTRIBUTE, "Expected 2 byte short, but got %lu bytes", (Lu)value.fill());
      short sval;
      memcpy(&sval, value.base, 2);
      cout << sval << endl;
    }
    else if (m_args[2].first == "int") {
      if (value.fill() != 4)
	HT_THROWF(Error::HYPERSPACE_BAD_ATTRIBUTE, "Expected 4 byte int, but got %lu bytes", (Lu)value.fill());
      uint32_t ival;
      memcpy(&ival, value.base, 4);
      cout << ival << endl;
    }
    else if (m_args[2].first == "long") {
      if (value.fill() != 8)
	HT_THROWF(Error::HYPERSPACE_BAD_ATTRIBUTE, "Expected 8 byte int, but got %lu bytes", (Lu)value.fill());
      uint64_t lval;
      memcpy(&lval, value.base, 8);
      cout << lval << endl;
    }
  }
  else {
    std::string valstr = std::string((const char *)value.base, value.fill());
    cout << valstr << endl;
  }

}
