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

using namespace Hypertable;
using namespace std;

/**
 *
 */
void dfsTestThreadFunction::operator()() {
  vector<const char *> args;
  int64_t origSize, dfsSize;
  CommandCopyFromLocal cmdCopyFromLocal(m_client);
  CommandCopyToLocal cmdCopyToLocal(m_client);
  CommandRemove cmdRemove(m_client);

  cmdCopyFromLocal.push_arg(m_input_file, "");
  cmdCopyFromLocal.push_arg(m_dfs_file, "");
  if (cmdCopyFromLocal.run() != 0)
    exit(1);

  cmdCopyToLocal.push_arg(m_dfs_file, "");
  cmdCopyToLocal.push_arg(m_output_file, "");
  if (cmdCopyToLocal.run() != 0)
    exit(1);

  // Determine original file size
  struct stat statbuf;
  if (stat(m_input_file.c_str(), &statbuf) != 0) {
    cerr << "Unable to stat file '" << m_input_file << "' - " << strerror(errno) << endl;
    exit(1);
  }
  origSize = statbuf.st_size;

  // Determine DFS file size
  m_client->length(m_dfs_file, &dfsSize);

  if (origSize != dfsSize) {
    HT_ERRORF("Length mismatch: %ld != %ld", origSize, dfsSize);
    exit(1);
  }

  /**
  cmdRemove.push_arg(m_dfs_file, "");
  if (cmdRemove.run() != 0)
    exit(1);
  **/
}
