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

#include "CommandLength.h"

using namespace std;
using namespace Hypertable;

const char *CommandLength::ms_usage[] = {
  "length <file>",
  "",
  "  This command sends a length request for the DFS file <file>",
  "  to the DfsBroker and prints the returned length",
  (const char *)0
};


void CommandLength::run() {

  if (m_args.size() < 1)
    HT_THROW(Error::PARSE_ERROR, "Error: no filename supplied");

  cout << m_client->length(m_args[0].first) << endl;

}

