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

#include "OperationRecoverServer.h"

using namespace Hypertable;

OperationRecoverServer::OperationRecoverServer(ContextPtr &context, RangeServerConnectionPtr &rsc)
  : Operation(context, MetaLog::EntityType::OPERATION_RECOVER_SERVER), m_rsc(rsc) {
  m_exclusivities.insert(m_rsc->location());
  m_dependencies.insert(Dependency::INIT);
}


void OperationRecoverServer::execute() {
  if (!m_rsc->connected())
    set_state(OperationState::BLOCKED);
  else {
    ScopedLock lock(m_mutex);
    m_state = OperationState::COMPLETE;
    m_completion_time = time(0);
  }
  HT_INFOF("Leaving RecoverServer-%lld('%s') state=%s",
           (Lld)header.id, m_rsc->location().c_str(), OperationState::get_text(get_state()));
}


void OperationRecoverServer::display_state(std::ostream &os) {
  os << " location=" << m_rsc->location() << " ";
}

const String OperationRecoverServer::name() {
  return "OperationRecoverServer";
}

const String OperationRecoverServer::label() {
  return String("RecoverServer ") + m_rsc->location();
}

