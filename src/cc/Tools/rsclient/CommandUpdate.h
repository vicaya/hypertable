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

#ifndef HYPERTABLE_COMMANDUPDATE_H
#define HYPERTABLE_COMMANDUPDATE_H

#include "Common/InteractiveCommand.h"

#include "Hypertable/Lib/MasterClient.h"
#include "Hypertable/Lib/RangeServerClient.h"

#include "Hyperspace/Session.h"

namespace Hypertable {

  class CommandUpdate : public InteractiveCommand {
  public:
    CommandUpdate(struct sockaddr_in &addr, RangeServerClientPtr &range_server_ptr, Hyperspace::SessionPtr &hyperspace_ptr) : m_addr(addr), m_range_server_ptr(range_server_ptr), m_hyperspace_ptr(hyperspace_ptr) { return; }
    virtual const char *command_text() { return "update"; }
    virtual const char **usage() { return ms_usage; }
    virtual int run();

  private:
    static const char *ms_usage[];

    struct sockaddr_in m_addr;
    RangeServerClientPtr m_range_server_ptr;
    Hyperspace::SessionPtr m_hyperspace_ptr;
  };

}

#endif // HYPERTABLE_COMMANDUPDATE_H
