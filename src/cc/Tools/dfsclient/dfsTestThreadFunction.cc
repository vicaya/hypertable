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
#include <vector>

extern "C" {
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
}

#include "Common/Logger.h"

#include "CommandCopyFromLocal.h"
#include "CommandCopyToLocal.h"
#include "CommandRemove.h"
#include "CommandLength.h"
#include "CommandMkdirs.h"
#include "dfsTestThreadFunction.h"

using namespace hypertable;
using namespace std;

/**
 *
 */
void dfsTestThreadFunction::operator()() {
  vector<const char *> args;
  int64_t origSize, dfsSize;
  CommandCopyFromLocal cmdCopyFromLocal(mClient);
  CommandCopyToLocal cmdCopyToLocal(mClient);
  CommandRemove cmdRemove(mClient);

  cmdCopyFromLocal.PushArg(mInputFile, "");
  cmdCopyFromLocal.PushArg(mDfsFile, "");
  if (cmdCopyFromLocal.run() != 0)
    exit(1);

  cmdCopyToLocal.PushArg(mDfsFile, "");
  cmdCopyToLocal.PushArg(mOutputFile, "");
  if (cmdCopyToLocal.run() != 0)
    exit(1);

  // Determine original file size
  struct stat statbuf;
  if (stat(mInputFile.c_str(), &statbuf) != 0) {
    cerr << "Unable to stat file '" << mInputFile << "' - " << strerror(errno) << endl;
    exit(1);
  }
  origSize = statbuf.st_size;

  // Determine DFS file size
  mClient->Length(mDfsFile, &dfsSize);

  if (origSize != dfsSize) {
    LOG_VA_ERROR("Length mismatch: %ld != %ld", origSize, dfsSize);
    exit(1);
  }

  /**
  cmdRemove.PushArg(mDfsFile, "");
  if (cmdRemove.run() != 0)
    exit(1);
  **/
}
