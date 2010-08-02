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

#include "Common/Compat.h"
#include <cassert>
#include <cstring>

#include "Common/Error.h"

#include "Hypertable/Lib/TableMutator.h"
#include "Hypertable/Lib/TableScanner.h"

#include "Global.h"
#include "MetadataNormal.h"

using namespace Hypertable;

/**
 *
 */
MetadataNormal::MetadataNormal(const TableIdentifier *identifier,
                               const String &end_row)
  : m_files_scanner(0) {
  m_metadata_key = String("") + identifier->id + ":" + end_row;
}



/**
 *
 */
MetadataNormal::~MetadataNormal() {
  m_files_scanner = 0;
}



void MetadataNormal::reset_files_scan() {
  ScanSpec scan_spec;
  RowInterval ri;

  scan_spec.row_limit = 1;
  scan_spec.max_versions = 1;
  ri.start = m_metadata_key.c_str();
  ri.end   = m_metadata_key.c_str();
  scan_spec.row_intervals.push_back(ri);
  scan_spec.columns.clear();
  scan_spec.columns.push_back("Files");

  m_files_scanner = Global::metadata_table->create_scanner(scan_spec);
}



bool MetadataNormal::get_next_files(String &ag_name, String &files) {
  Cell cell;

  assert(m_files_scanner);

  if (m_files_scanner->next(cell)) {
    assert(!strcmp(cell.column_family, "Files"));
    ag_name = String(cell.column_qualifier);
    files = String((const char *)cell.value, cell.value_len);
    return true;
  }

  m_files_scanner = 0;
  return false;
}



void MetadataNormal::write_files(const String &ag_name, const String &files) {
  TableMutatorPtr mutator;
  KeySpec key;

  mutator = Global::metadata_table->create_mutator();

  key.row = m_metadata_key.c_str();
  key.row_len = m_metadata_key.length();
  key.column_family = "Files";
  key.column_qualifier = ag_name.c_str();
  key.column_qualifier_len = ag_name.length();
  mutator->set(key, (uint8_t *)files.c_str(), files.length());
  mutator->flush();
}
