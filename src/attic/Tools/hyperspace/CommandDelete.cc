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


extern "C" {
#include <stdio.h>
#include <string.h>
}

#include "Common/Usage.h"

#include "Hyperspace/HyperspaceClient.h"

#include "CommandDelete.h"

using namespace Hypertable;

namespace {
  const char *usage[] = {
    "",
    "usage: hyperspaceClient touch <fname>",
    "",
    "This program deletes a file named <fname> in HYPERSPACE if it does",
    "not already exist.",
    "",
    (const char *)0
  };

}

int Hypertable::CommandDelete(HyperspaceClient *client, vector<const char *> &args) {

  if (args.size() < 1)
    Usage::DumpAndExit(usage);

  return client->Delete(args[0]);

}

