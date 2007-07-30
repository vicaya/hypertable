/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */


#include "Common/Error.h"
#include "Common/Usage.h"

#include "Global.h"
#include "CommandRemove.h"

using namespace hypertable;

namespace {
  const char *usage[] = {
    "usage: hdfsClient remove <file>",
    "",
    "This command sends a remove request for the HDFS file <file> to the HdfsBroker.",
    (const char *)0
  };

}

int hypertable::CommandRemove(vector<const char *> &args) {
  if (args.size() < 1)
    Usage::DumpAndExit(usage);

  return Global::client->Remove(args[0]);
}

