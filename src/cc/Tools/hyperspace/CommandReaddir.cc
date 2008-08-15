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

#include "Hyperspace/DirEntry.h"

#include "CommandReaddir.h"
#include "Global.h"
#include "Util.h"

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

const char *CommandReaddir::ms_usage[] = {
  "readdir <dir>",
  "  This command issues a READDIR request to Hyperspace.",
  (const char *)0
};


void CommandReaddir::run() {
  uint64_t handle;
  std::vector<struct DirEntry> listing;

  if (m_args.size() != 1)
    HT_THROW(Error::COMMAND_PARSE_ERROR,
             "Wrong number of arguments.  Type 'help' for usage.");

  if (m_args[0].second != "")
    HT_THROW(Error::COMMAND_PARSE_ERROR, "Invalid character '=' in argument.");

  handle = Util::get_handle(m_args[0].first);

  m_session->readdir(handle, listing);

  struct LtDirEntry ascending;
  sort(listing.begin(), listing.end(), ascending);
  for (size_t i=0; i<listing.size(); i++) {
    if (listing[i].is_dir)
      cout << "(dir) ";
    else
      cout << "      ";
    cout << listing[i].name << endl;
  }

}
