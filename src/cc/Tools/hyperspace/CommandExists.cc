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

#include "Common/Error.h"
#include "Common/Usage.h"

#include "CommandExists.h"
#include "Global.h"

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

const char *CommandExists::ms_usage[] = {
  "exists <name>",
  "  This command issues a EXISTS request to Hyperspace.",
  (const char *)0
};

int CommandExists::run() {
  int error;
  bool exists;

  if (m_args.size() != 1) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if (m_args[0].second != "") {
    cerr << "Invalid argument - " << m_args[0].second << endl;
    return -1;
  }

  if ((error = m_session->exists(m_args[0].first, &exists)) == Error::OK) {
    if (exists)
      cout << "true" << endl;
    else {
      cout << "false" << endl;
      Global::exitStatus = 1;
    }
  }

  return error;
}
