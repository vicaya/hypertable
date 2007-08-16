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
#include "Common/FileUtils.h"
#include "Common/Usage.h"

#include "CommandGetSchema.h"

using namespace hypertable;
using namespace std;

const char *CommandGetSchema::msUsage[] = {
  "get schema <tableName>",
  "",
  "  This command fetches the schema for table <tableName> and displays it to stdout.",
  (const char *)0
};

int CommandGetSchema::run() {
  off_t len;
  std::string schema;
  int error;

  if (mArgs.size() != 1) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if (mArgs[0].second != "")
    Usage::DumpAndExit(msUsage);

  if ((error = mManager->GetSchema(mArgs[0].first, schema)) != Error::OK) {
    cerr << "Problem getting schema for table '" << mArgs[0].first << "' - " << Error::GetText(error) << endl;
    return error;
  }

  cout << schema << endl;

  return Error::OK;
}
