/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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

#ifndef HYPERTABLE_MASTERCOMMANDINTERPRETER_H
#define HYPERTABLE_MASTERCOMMANDINTERPRETER_H

#include "Common/String.h"

#include "AsyncComm/Comm.h"

#include "Tools/Lib/CommandInterpreter.h"

#include "Hypertable/Lib/MasterClient.h"

namespace Hypertable {

  class MasterCommandInterpreter : public CommandInterpreter {
  public:
    MasterCommandInterpreter(Comm *, const sockaddr_in addr, MasterClientPtr &);

    virtual void execute_line(const String &line);

  private:
    Comm *m_comm;
    struct sockaddr_in m_addr;
    MasterClientPtr m_master;
  };

  typedef intrusive_ptr<MasterCommandInterpreter>
          MasterCommandInterpreterPtr;

}

#endif // HYPERTABLE_MASTERCOMMANDINTERPRETER_H
