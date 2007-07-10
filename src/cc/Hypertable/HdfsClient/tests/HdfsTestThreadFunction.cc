/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
