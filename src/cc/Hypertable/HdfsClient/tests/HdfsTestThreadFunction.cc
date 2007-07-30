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


#include <iostream>
#include <vector>

extern "C" {
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
}

#include "Common/Logger.h"

#include "../CommandCopyFromLocal.h"
#include "../CommandCopyToLocal.h"
#include "../CommandRemove.h"
#include "../CommandLength.h"
#include "HdfsTestThreadFunction.h"

using namespace hypertable;
using namespace hypertable;
using namespace std;

/**
 *
 */
void HdfsTestThreadFunction::operator()() {
  vector<const char *> args;
  int64_t origSize, hdfsSize;

  args.push_back(mInputFile);
  args.push_back(mHdfsFile);

  if (CommandCopyFromLocal(args) != 0)
    exit(1);

  args.clear();
  args.push_back(mHdfsFile);
  args.push_back(mOutputFile);

  if (CommandCopyToLocal(args) != 0)
    exit(1);

  // Check size
  struct stat statbuf;
  if (stat(mInputFile, &statbuf) != 0) {
    cerr << "Unable to stat file '" << mInputFile << "' - " << strerror(errno) << endl;
    exit(1);
  }
  origSize = statbuf.st_size;

  args.clear();
  args.push_back(mHdfsFile);
  if ((hdfsSize = CommandLength(args)) == -1)
    exit(1);

  if (origSize != hdfsSize) {
    LOG_VA_ERROR("Length mismatch: %ld != %ld", origSize, hdfsSize);
    exit(1);
  }

  args.clear();
  args.push_back(mHdfsFile);
  if (CommandRemove(args) != 0)
    exit(1);
}
