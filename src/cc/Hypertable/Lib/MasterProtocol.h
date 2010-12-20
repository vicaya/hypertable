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

#ifndef MASTER_PROTOCOL_H
#define MASTER_PROTOCOL_H

#include "Common/StatsSystem.h"

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/Protocol.h"

#include "Hypertable/Lib/Types.h"


namespace Hypertable {

  class MasterProtocol : public Protocol {

  public:

    static const uint64_t COMMAND_CREATE_TABLE          = 0;
    static const uint64_t COMMAND_GET_SCHEMA            = 1;
    static const uint64_t COMMAND_STATUS                = 2;
    static const uint64_t COMMAND_REGISTER_SERVER       = 3;
    static const uint64_t COMMAND_MOVE_RANGE            = 4;
    static const uint64_t COMMAND_DROP_TABLE            = 5;
    static const uint64_t COMMAND_ALTER_TABLE           = 6;
    static const uint64_t COMMAND_SHUTDOWN              = 7;
    static const uint64_t COMMAND_CLOSE                 = 8;
    static const uint64_t COMMAND_CREATE_NAMESPACE      = 9;
    static const uint64_t COMMAND_DROP_NAMESPACE        = 10;
    static const uint64_t COMMAND_RENAME_TABLE          = 11;
    static const uint64_t COMMAND_MAX                   = 12;

    static const char *m_command_strings[];

    static CommBuf *
    create_create_namespace_request(const String &name, int flags);
    static CommBuf *
    create_drop_namespace_request(const String &name, bool if_exists);
    static CommBuf *
    create_create_table_request(const String &tablename, const String &schemastr);
    static CommBuf *
    create_alter_table_request(const String &tablename, const String &schemastr);

    static CommBuf *create_get_schema_request(const String &tablename);

    static CommBuf *create_status_request();

    static CommBuf *create_register_server_request(const String &location,
                                                   StatsSystem &system_stats);

    static CommBuf *
    create_move_range_request(const TableIdentifier *, const RangeSpec &,
                              const String &transfer_log_dir,
                              uint64_t soft_limit, bool split);
    static CommBuf *create_rename_table_request(const String &old_name, const String &new_name);
    static CommBuf *create_drop_table_request(const String &table_name,
                                              bool if_exists);

    static CommBuf *create_close_request();

    static CommBuf *create_shutdown_request();

    virtual const char *command_text(uint64_t command);

  };

}

#endif // MASTER_PROTOCOL_H
