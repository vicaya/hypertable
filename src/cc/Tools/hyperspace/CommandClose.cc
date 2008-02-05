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

#include "CommandClose.h"
#include "Global.h"
#include "Util.h"

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

const char *CommandClose::ms_usage[] = {
  "close <file>",
  "  This command issues a CLOSE request to Hyperspace.",
  (const char *)0
};

int CommandClose::run() {
  uint64_t handle;
  int error;

  if (m_args.size() != 1) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if (m_args[0].second != "") {
    cerr << "Invalid character '=' in argument." << endl;
    return -1;
  }

  if (!Util::get_handle(m_args[0].first, &handle))
    return -1;

  if ((error = m_session->close(handle)) != Error::OK) {
    HT_ERRORF("Error executing CLOSE request - %s", Error::get_text(error));
  }

  return error;
}
