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

#include "CommandAttrDel.h"

using namespace Hypertable;

namespace {
  const char *usage[] = {
    "",
    "usage: hyperspace attrdel <fname> <attrname>",
    "",
    "This program removes the extended attribute <attrname> from file <fname>",
    "",
    (const char *)0
  };

}


int Hypertable::CommandAttrDel(HyperspaceClient *client, vector<const char *> &args) {

  if (args.size() != 2)
    Usage::DumpAndExit(usage);

  return client->AttrDel(args[0], args[1]);
  
}

