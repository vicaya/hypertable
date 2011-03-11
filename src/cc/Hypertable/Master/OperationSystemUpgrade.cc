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

#include "Common/Compat.h"
#include "Common/Error.h"
#include "Common/FailureInducer.h"
#include "Common/ScopeGuard.h"
#include "Common/System.h"

#include "OperationSystemUpgrade.h"
#include "Utility.h"

using namespace Hypertable;
using namespace Hyperspace;

OperationSystemUpgrade::OperationSystemUpgrade(ContextPtr &context)
  : Operation(context, MetaLog::EntityType::OPERATION_SYSTEM_UPGRADE) {
}

OperationSystemUpgrade::OperationSystemUpgrade(ContextPtr &context,
                                               const MetaLog::EntityHeader &header_)
  : Operation(context, header_) {
}


bool OperationSystemUpgrade::update_schema(const String &name, const String &schema_file) {
  uint64_t handle = 0;
  String table_id;
  SchemaPtr old_schema, new_schema;
  DynamicBuffer value_buf;

  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_context->hyperspace, &handle);

  if (!m_context->namemap->name_to_id(name, table_id))
    return false;

  String tablefile = m_context->toplevel_dir + "/tables/" + table_id;

  handle = m_context->hyperspace->open(tablefile, OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_LOCK_EXCLUSIVE);
  if (!m_context->hyperspace->attr_exists(handle, "schema"))
    return false;

  m_context->hyperspace->attr_get(handle, "schema", value_buf);
  old_schema = Schema::new_instance((char *)value_buf.base,
                                    strlen((char *)value_buf.base));

  String schema_str = FileUtils::file_to_string( System::install_dir + schema_file );
  new_schema = Schema::new_instance(schema_str.c_str(), schema_str.length());
  
  if (new_schema->need_id_assignment())
    HT_THROW(Error::SCHEMA_PARSE_ERROR, "conf/METADATA.xml missing ID assignment");

  if (old_schema->get_generation() < new_schema->get_generation()) {
    m_context->hyperspace->attr_set(handle, "schema", schema_str.c_str(),
                           schema_str.length());
    HT_INFOF("Upgraded schema for '%s' from generation %u to %u", name.c_str(),
             old_schema->get_generation(), new_schema->get_generation());
    return true;
  }
  return false;
}


void OperationSystemUpgrade::execute() {
  int32_t state = get_state();

  HT_INFOF("Entering SystemUpgrade-%lld", (Lld)header.id);

  switch (state) {

  case OperationState::INITIAL:
    update_schema("/sys/METADATA",   "/conf/METADATA.xml");
    update_schema("/sys/RS_METRICS", "/conf/RS_METRICS.xml");
    {
      ScopedLock lock(m_mutex);
      m_state = OperationState::COMPLETE;
      m_completion_time = time(0);
    }
    break;

  default:
    HT_FATALF("Unrecognized state %d", state);
  }

  HT_INFOF("Leaving SystemUpgrade-%lld", (Lld)header.id);
}

const String OperationSystemUpgrade::name() {
  return "OperationSystemUpgrade";
}

const String OperationSystemUpgrade::label() {
  return String("SystemUpgrade");
}

