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
#include "Common/Usage.h"

#include "CommandCopyToLocal.h"

using namespace Hypertable;

namespace {
  const int BUFFER_SIZE = 32768;
}


const char *CommandCopyToLocal::ms_usage[] = {
  "copyToLocal [--seek=<n>] <src> <dst>",
  "",
  "  This program copies the file <src> from the DFS to the file",
  "  <dst> in the local filesystem.  If --seek is supplied, then",
  "  the source file is seeked to offset <n> before starting the copy.",
  (const char *)0
};


int CommandCopyToLocal::run() {
  DispatchHandlerSynchronizer syncHandler;
  int32_t fd = 0;
  int error = Error::OK;
  EventPtr eventPtr;
  FILE *fp = 0;
  uint64_t startOffset = 0;
  int srcArg = 0;
  uint64_t offset;
  uint32_t amount;
  uint8_t *dst;

  if (m_args.size() < 2) {
    cerr << "Insufficient number of arguments" << endl;
    return -1;
  }

  if (!strcmp(m_args[0].first.c_str(), "--seek") && m_args[0].second != "") {
    startOffset = strtol(m_args[0].second.c_str(), 0, 10);
    srcArg = 1;
  }

  if ((fp = fopen(m_args[srcArg+1].first.c_str(), "w+")) == 0) {
    perror(m_args[srcArg+1].first.c_str());
    goto abort;
  }

  if ((error = m_client->open(m_args[srcArg].first, &fd)) != Error::OK)
    goto abort;

  if (startOffset > 0) {
    if ((error = m_client->seek(fd, startOffset)) != Error::OK)
      goto abort;
  }

  if ((error = m_client->read(fd, BUFFER_SIZE, &syncHandler)) != Error::OK)
    goto abort;

  if ((error = m_client->read(fd, BUFFER_SIZE, &syncHandler)) != Error::OK)
    goto abort;

  if ((error = m_client->read(fd, BUFFER_SIZE, &syncHandler)) != Error::OK)
    goto abort;

  while (syncHandler.wait_for_reply(eventPtr)) {

    if ((error = Filesystem::decode_response_read_header(eventPtr, &offset, &amount, &dst)) != Error::OK)
      goto abort;

    if (amount > 0) {
      if (fwrite(dst, amount, 1, fp) != 1) {
	perror("write failed");
	goto abort;
      }
    }

    if (amount < (uint32_t)BUFFER_SIZE) {
      syncHandler.wait_for_reply(eventPtr);
      break;
    }

    if ((error = m_client->read(fd, BUFFER_SIZE, &syncHandler)) != Error::OK)
      goto abort;
  }

  if ((error = m_client->close(fd)) != Error::OK)
    goto abort;

  abort:
  if (fp)
    fclose(fp);
  if (error != 0)
    cerr << Error::get_text(error) << endl;
  return error;
}

