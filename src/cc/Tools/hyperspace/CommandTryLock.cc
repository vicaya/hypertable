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
#include <iostream>

#include "Common/Error.h"
#include "Common/Usage.h"

#include "CommandTryLock.h"
#include "Global.h"
#include "Util.h"

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

const char *CommandTryLock::ms_usage[] = {
  "trylock <file> <mode>",
  "  This command issues a TRYLOCK request to Hyperspace.  The <mode> argument",
  "  may be either SHARED or EXCLUSIVE.",
  (const char *)0
};

void CommandTryLock::run() {
  uint64_t handle;
  uint32_t mode = 0;
  struct LockSequencer lockseq;
  uint32_t status;

  if (m_args.size() != 2)
    HT_THROW(Error::PARSE_ERROR, "Wrong number of arguments.  Type 'help' for usage.");

  if (m_args[0].second != "" || m_args[1].second != "")
    HT_THROW(Error::PARSE_ERROR, "Invalid character '=' in argument.");

  if (m_args[1].first == "SHARED")
    mode = LOCK_MODE_SHARED;
  else if (m_args[1].first == "EXCLUSIVE")
    mode = LOCK_MODE_EXCLUSIVE;
  else
    HT_THROWF(Error::PARSE_ERROR, "Invalid mode value (%s)", m_args[1].second.c_str());

  handle = Util::get_handle(m_args[0].first);

  m_session->try_lock(handle, mode, &status, &lockseq);

  if (status == LOCK_STATUS_GRANTED)
    cout << "SEQUENCER name=" << lockseq.name << " mode=" << lockseq.mode << " generation=" << lockseq.generation << endl << flush;
  else if (status == LOCK_STATUS_BUSY)
    cout << "busy" << endl;
  else
    cout << "Unknown status: " << status << endl;
}
