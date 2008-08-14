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

void CommandExists::run() {

  if (m_args.size() != 1)
    HT_THROW(Error::PARSE_ERROR, "Wrong number of arguments.  Type 'help' for usage.");

  if (m_args[0].second != "")
    HT_THROWF(Error::PARSE_ERROR, "Invalid argument - %s", m_args[0].second.c_str());

  if (m_session->exists(m_args[0].first))
    cout << "true" << endl;
  else {
    cout << "false" << endl;
    Global::exit_status = 1;
  }

}
