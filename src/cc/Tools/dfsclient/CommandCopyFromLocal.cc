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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
extern "C" {
#include <stdio.h>
}

#include "Common/Error.h"
#include "Common/Logger.h"

#include "CommandCopyFromLocal.h"

using namespace std;
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


void CommandCopyFromLocal::run() {
  DispatchHandlerSynchronizer sync_handler;
  int32_t fd = 0;
  EventPtr event_ptr;
  FILE *fp = 0;
  size_t nread;
  int src_arg = 0;
  uint8_t *buf;
  StaticBuffer send_buf;

  if (m_args.size() != 2)
    HT_THROW(Error::COMMAND_PARSE_ERROR,
             "Wrong number of arguments.  Type 'help' for usage.");

  try {

    if ((fp = fopen(m_args[src_arg].first.c_str(), "r")) == 0)
      HT_THROW(Error::EXTERNAL, strerror(errno));

    fd = m_client->create(m_args[src_arg+1].first, Filesystem::OPEN_FLAG_OVERWRITE, -1, -1, -1);

    // send 3 appends
    for (int i=0; i<3; i++) {
      buf = new uint8_t [BUFFER_SIZE];
      if ((nread = fread(buf, 1, BUFFER_SIZE, fp)) == 0)
        goto done;
      send_buf.set(buf, nread, true);
      m_client->append(fd, send_buf, 0, &sync_handler);
    }

    while (true) {

      if (!sync_handler.wait_for_reply(event_ptr))
        HT_THROW(event_ptr->error, event_ptr->to_str());

      buf = new uint8_t [BUFFER_SIZE];
      if ((nread = fread(buf, 1, BUFFER_SIZE, fp)) == 0)
        break;
      send_buf.set(buf, nread, true);
      m_client->append(fd, send_buf, 0, &sync_handler);
    }

  done:
    m_client->close(fd);

  }
  catch (Exception &e) {
    if (fp)
      fclose(fp);
    HT_THROW(e.code(), e.what());
  }
}
