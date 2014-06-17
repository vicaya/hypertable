/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_RANGESERVERCOMMANDINTERPRETER_H
#define HYPERTABLE_RANGESERVERCOMMANDINTERPRETER_H

#include "Common/String.h"

#include "AsyncComm/Comm.h"

#include "Hyperspace/Session.h"

#include "Tools/Lib/CommandInterpreter.h"

#include "Hypertable/Lib/NameIdMapper.h"
#include "Hypertable/Lib/RangeServerClient.h"
#include "Hypertable/Lib/SerializedKey.h"

#include "TableInfo.h"

namespace Hypertable {

  class Client;

  class RangeServerCommandInterpreter : public CommandInterpreter {
  public:
    RangeServerCommandInterpreter(Comm *, Hyperspace::SessionPtr &,
        const sockaddr_in addr, RangeServerClientPtr &);

    virtual void execute_line(const String &line);

  private:

    void display_scan_data(const SerializedKey &key, const ByteString &value,
                           SchemaPtr &schema_ptr);

    Comm *m_comm;
    Hyperspace::SessionPtr m_hyperspace;
    NameIdMapperPtr         m_namemap;
    struct sockaddr_in m_addr;
    RangeServerClientPtr m_range_server;
    typedef hash_map<String, TableInfo *> TableMap;
    TableMap m_table_map;
    int32_t m_cur_scanner_id;
    String m_toplevel_dir;
  };

  typedef intrusive_ptr<RangeServerCommandInterpreter>
          RangeServerCommandInterpreterPtr;

}

#endif // HYPERTABLE_RANGESERVERCOMMANDINTERPRETER_H
