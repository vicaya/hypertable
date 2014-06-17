/**
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_LOADTHREAD_H
#define HYPERTABLE_LOADTHREAD_H

#include <vector>

#include "Hypertable/Lib/Table.h"
#include "Hypertable/Lib/TableMutator.h"

#include "ParallelLoad.h"

namespace Hypertable {

  class LoadThread {

  public:
  LoadThread(TablePtr &table, ::uint32_t mutator_flags, ParallelStateRec &state)
    : m_table(table), m_mutator_flags(mutator_flags), m_state(state) { }
    void operator()();

  private:
    TablePtr m_table;
    TableMutatorPtr m_mutator;
    ::uint32_t m_mutator_flags;
    ParallelStateRec &m_state;
  };

}

#endif // HYPERTABLE_LOADTHREAD_H
