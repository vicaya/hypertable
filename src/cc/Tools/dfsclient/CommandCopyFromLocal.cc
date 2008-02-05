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
}

#include "Common/Error.h"
#include "Common/Logger.h"

#include "CommandCopyFromLocal.h"

using namespace Hypertable;

namespace {
  const int BUFFER_SIZE = 32768;
}


const char *CommandCopyFromLocal::ms_usage[] = {
  "copyFromLocal <src> <dst>",
  "",
  "  This program copies the local file <src> to the remote",
  "  file <dst> in the DFS.",
  (const char *)0
};


int CommandCopyFromLocal::run() {
  DispatchHandlerSynchronizer syncHandler;
  int32_t fd = 0;
  int error = Error::OK;
  EventPtr eventPtr;
  FILE *fp = 0;
  size_t nread;
  int srcArg = 0;
  uint8_t *buf;

  if (m_args.size() != 2) {
    cerr << "Wrong number of arguments.  Type 'help' for usage." << endl;
    return -1;
  }

  if ((fp = fopen(m_args[srcArg].first.c_str(), "r")) == 0) {
    perror(m_args[srcArg].first.c_str());
    goto abort;
  }

  if ((error = m_client->create(m_args[srcArg+1].first, true, -1, -1, -1, &fd)) != Error::OK)
    goto abort;

  // send 3 appends
  for (int i=0; i<3; i++) {
    buf = new uint8_t [ BUFFER_SIZE ];
    if ((nread = fread(buf, 1, BUFFER_SIZE, fp)) == 0)
      goto done;
    if ((error = m_client->append(fd, buf, nread, &syncHandler)) != Error::OK)
      goto done;
  }

  while (true) {

    if (!syncHandler.wait_for_reply(eventPtr)) {
      HT_ERRORF("%s", eventPtr->toString().c_str());
      goto abort;
    }

    buf = new uint8_t [ BUFFER_SIZE ];
    if ((nread = fread(buf, 1, BUFFER_SIZE, fp)) == 0)
      break;
    if ((error = m_client->append(fd, buf, nread, &syncHandler)) != Error::OK)
      goto abort;
  }

 done:

  if ((error = m_client->close(fd)) != Error::OK)
    goto abort;
    
  abort:
  if (fp)
    fclose(fp);
  if (error != 0)
    cerr << Error::get_text(error) << endl;
  return error;
}
