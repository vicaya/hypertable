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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

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


int CommandAttrGet::run() {
  uint64_t handle;
  int error;
  DynamicBuffer value(0);

  if (m_args.size() < 2 || m_args.size() > 3) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if (m_args[0].second != "" || m_args[1].second != "") {
    cerr << "Invalid character '=' in argument." << endl;
    return -1;
  }

  if (!Util::get_handle(m_args[0].first, &handle))
    return -1;

  if ((error = m_session->attr_get(handle, m_args[1].first, value)) == Error::OK) {
    if (m_args.size() == 3) {
      if (m_args[2].first == "short") {
	if (value.fill() != 2) {
	  cerr << "Expected 2 byte short, but got " << value.fill() << " bytes" << endl;
	  return Error::HYPERSPACE_BAD_ATTRIBUTE;
	}
	short sval;
	memcpy(&sval, value.buf, 2);
	cout << sval << endl;
      }
      else if (m_args[2].first == "int") {
	if (value.fill() != 4) {
	  cerr << "Expected 4 byte int, but got " << value.fill() << " bytes" << endl;
	  return Error::HYPERSPACE_BAD_ATTRIBUTE;
	}
	uint32_t ival;
	memcpy(&ival, value.buf, 4);
	cout << ival << endl;
      }
      else if (m_args[2].first == "long") {
	if (value.fill() != 8) {
	  cerr << "Expected 8 byte int, but got " << value.fill() << " bytes" << endl;
	  return Error::HYPERSPACE_BAD_ATTRIBUTE;
	}
	uint64_t lval;
	memcpy(&lval, value.buf, 8);
	cout << lval << endl;
      }
    }
    else {
      std::string valStr = std::string((const char *)value.buf, value.fill());
      cout << valStr << endl;
    }
  }

  return error;
}
