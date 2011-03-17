/** -*- c++ -*-
 * Copyright (C) 2010 Hypertable, Inc.
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
#include "Common/Compat.h"

#include "MetaLogDefinitionMaster.h"

#include "OperationAlterTable.h"
#include "OperationCreateNamespace.h"
#include "OperationCreateTable.h"
#include "OperationDropTable.h"
#include "OperationDropNamespace.h"
#include "OperationInitialize.h"
#include "OperationMoveRange.h"
#include "OperationRenameTable.h"
#include "RangeServerConnection.h"

using namespace Hypertable;
using namespace Hypertable::MetaLog;

uint16_t DefinitionMaster::version() {
  return 1;
}

const char *DefinitionMaster::name() {
  return "mml";
}

Entity *DefinitionMaster::create(const EntityHeader &header) {

  if (header.type == EntityType::RANGE_SERVER_CONNECTION) {
    MetaLog::WriterPtr mml_writer = m_context ? m_context->mml_writer : 0;
    return new RangeServerConnection(mml_writer, header);
  }
  else if (header.type == EntityType::OPERATION_INITIALIZE)
    return new OperationInitialize(m_context, header);
  else if (header.type == EntityType::OPERATION_ALTER_TABLE)
    return new OperationAlterTable(m_context, header);
  else if (header.type == EntityType::OPERATION_CREATE_NAMESPACE)
    return new OperationCreateNamespace(m_context, header);
  else if (header.type == EntityType::OPERATION_DROP_NAMESPACE)
    return new OperationDropNamespace(m_context, header);
  else if (header.type == EntityType::OPERATION_CREATE_TABLE)
    return new OperationCreateTable(m_context, header);
  else if (header.type == EntityType::OPERATION_DROP_TABLE)
    return new OperationDropTable(m_context, header);
  else if (header.type == EntityType::OPERATION_RENAME_TABLE)
    return new OperationRenameTable(m_context, header);
  else if (header.type == EntityType::OPERATION_MOVE_RANGE)
    return new OperationMoveRange(m_context, header);

  HT_THROWF(Error::METALOG_ENTRY_BAD_TYPE,
            "Unrecognized type (%d) encountered in mml",
            (int)header.type);

}

