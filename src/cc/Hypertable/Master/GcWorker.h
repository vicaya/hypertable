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

#ifndef HYPERTABLE_GCWORKER_H
#define HYPERTABLE_GCWORKER_H

#include "Common/CstrHashMap.h"

#include "Hypertable/Lib/Client.h"

#include "Context.h"

namespace Hypertable {

  typedef CstrHashMap<int> CountMap; // filename -> reference count

  class GcWorker {
  public:
    GcWorker(ContextPtr &context);
    void gc();
    
  private:
    void scan_metadata(CountMap &files_map);
    void delete_row(const std::string &row, TableMutatorPtr &mutator);
    void delete_cell(const Cell &cell, TableMutatorPtr &mutator);
    void insert_files(CountMap &map, const char *buf, size_t len, int c=0);
    void insert_file(CountMap &map, const char *fname, int c);
    void reap(CountMap &files_map);

    ContextPtr m_context;
    String     m_tables_dir;
  };

} // namespace Hypertable

#endif // HYPERTABLE_GCWORKER_H
