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

#include <cstdlib>
#include <cstring>
#include <iostream>

#include "Common/Error.h"

#include "CommandShutdown.h"

using namespace Hypertable;
using namespace std;

const char *CommandShutdown::ms_usage[] = {
  "shutdown",
  "",
  "  This command issues a SHUTDOWN command to the range server which causes it",
  "  to call exit.",
  (const char *)0
};



int CommandShutdown::run() {
  int error;

  if ((error = m_range_server_ptr->shutdown(m_addr)) != Error::OK)
    return error;

  return Error::OK;
}
