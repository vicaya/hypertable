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

#include "OperationCollectGarbage.h"
#include "GcWorker.h"

using namespace Hypertable;

OperationCollectGarbage::OperationCollectGarbage(ContextPtr &context)
  : Operation(context, MetaLog::EntityType::OPERATION_COLLECT_GARBAGE) {
  m_dependencies.insert(Dependency::INIT);
  m_dependencies.insert(Dependency::METADATA);
}


void OperationCollectGarbage::execute() {
  HT_INFOF("Entering CollectGarbage-%lld", (Lld)header.id);
  try {
    GcWorker worker(m_context);
    worker.gc();
  }
  catch (Exception &e) {
    HT_THROW2(e.code(), e, "Garbage Collection");
  }
  set_state(OperationState::COMPLETE);
  HT_INFOF("Leaving CollectGarbage-%lld", (Lld)header.id);
}

const String OperationCollectGarbage::name() {
  return "OperationCollectGarbage";
}

const String OperationCollectGarbage::label() {
  return "CollectGarbage";
}

