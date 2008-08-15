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
#include "Common/Error.h"

#include "CommandExists.h"

using namespace std;
using namespace Hypertable;

const char *CommandExists::ms_usage[] = {
  "exists <file>",
  "",
  "  This command sends an exists request for the DFS file <file>",
  "  to the DfsBroker and prints the result.",
  (const char *)0
};


void CommandExists::run() {

  if (m_args.size() < 1)
    HT_THROW(Error::COMMAND_PARSE_ERROR, "Error: no filename supplied");

  if (m_client->exists(m_args[0].first))
    cout << "true" << endl;
  else
    cout << "false" << endl;

}

