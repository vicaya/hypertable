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

extern "C" {
#include <stdio.h>
#include <string.h>
}

#include "Common/DynamicBuffer.h"
#include "Common/Error.h"
#include "Common/Usage.h"

#include "Hyperspace/HyperspaceClient.h"

#include "CommandAttrGet.h"

using namespace Hypertable;


namespace {
  const char *usage[] = {
    "",
    "usage: hyperspace attrget <fname> <attrname>",
    "",
    "This program gets the extended attribute <attrname> for file <fname>",
    "",
    (const char *)0
  };

}

int Hypertable::CommandAttrGet(HyperspaceClient *client, vector<const char *> &args) {
  DynamicBuffer valueBuf(0);

  if (args.size() != 2)
    Usage::DumpAndExit(usage);

  int error = client->AttrGet(args[0], args[1], valueBuf);

  if (error != Error::OK)
    return error;

  if (valueBuf.fill() == 0)
    cout << "[NULL]" << endl;
  else
    cout << (char *)valueBuf.buf << endl;

  return error;
}

