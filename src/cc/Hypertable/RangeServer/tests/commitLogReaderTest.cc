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
#include <string>

#include "Common/Logger.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "../CommitLogReaderLocal.h"

using namespace hypertable;
using namespace std;

namespace {
  const char *usage[] = {
    "usage: commitLogReaderTest <commitLogDir>",
    "",
    0
  };
}


int main(int argc, char **argv) {
  string logDir;
  CommitLogHeaderT *header;  

  System::Initialize(argv[0]);
  
  if (argc != 2)
    Usage::DumpAndExit(usage);

  logDir = argv[1];
  std::string dummy = "";

  CommitLogReader *clogReader = new CommitLogReaderLocal(logDir, dummy);

  clogReader->InitializeRead(0);

  while (clogReader->NextBlock(&header)) {
    cout << "Block table='" << (char *)&header[1] << "' size=" << header->length << " timestamp=" << header->timestamp << endl;
  }

  return 0;
}
