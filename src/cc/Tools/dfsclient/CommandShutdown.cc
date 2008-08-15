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
#include "Common/Error.h"
#include "Common/Usage.h"

#include "DfsBroker/Lib/Protocol.h"

#include "CommandShutdown.h"

using namespace Hypertable;

const char *CommandShutdown::ms_usage[] = {
  "shutdown [now]",
  "",
  "  This command sends a shutdown request to the DfsBroker",
  "  server.  If the 'now' argument is given, the DfsBroker",
  "  will do an unclean shutdown by exiting immediately.  Otherwise",
  "  it will wait for all pending requests to complete before",
  "  shutting down.",
  (const char *)0
};


void CommandShutdown::run() {
  DispatchHandlerSynchronizer sync_handler;
  uint16_t flags = 0;
  EventPtr event_ptr;

  if (m_args.size() > 0) {
    if (m_args[0].first == "now")
      flags |= DfsBroker::Protocol::SHUTDOWN_FLAG_IMMEDIATE;
    else
      HT_THROWF(Error::COMMAND_PARSE_ERROR, "Invalid argument - %s",
                m_args[0].first.c_str());
  }

  m_client->shutdown(flags, &sync_handler);

  sync_handler.wait_for_reply(event_ptr);

}


