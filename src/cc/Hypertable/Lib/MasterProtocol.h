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

#ifndef MASTER_PROTOCOL_H
#define MASTER_PROTOCOL_H

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/Protocol.h"

#include "Hypertable/Lib/Types.h"

using namespace Hypertable;

namespace Hypertable {

  class MasterProtocol : public Protocol {

  public:

    static const short COMMAND_CREATE_TABLE    = 0;
    static const short COMMAND_GET_SCHEMA      = 1;
    static const short COMMAND_STATUS          = 2;
    static const short COMMAND_REGISTER_SERVER = 3;
    static const short COMMAND_REPORT_SPLIT    = 4;
    static const short COMMAND_DROP_TABLE      = 5;
    static const short COMMAND_MAX             = 6;

    static const char *m_command_strings[];

    static CommBuf *create_create_table_request(const char *tableName, const char *schemaString);

    static CommBuf *create_get_schema_request(const char *tableName);

    static CommBuf *create_status_request();

    static CommBuf *create_register_server_request(std::string &location);

    static CommBuf *create_report_split_request(TableIdentifierT &table, RangeT &range, uint64_t soft_limit);

    static CommBuf *create_drop_table_request(const char *table_name, bool if_exists);

    virtual const char *command_text(short command);
    
  };

}

#endif // MASTER_PROTOCOL_H
