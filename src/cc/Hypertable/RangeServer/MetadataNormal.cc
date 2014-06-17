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
                               const String &end_row) {
  m_metadata_key = String("") + identifier->id + ":" + end_row;
}



/**
 *
 */
MetadataNormal::~MetadataNormal() {
}



void MetadataNormal::reset_files_scan() {
  ScanSpec scan_spec;
  RowInterval ri;
  Cell cell;

  m_ag_map.clear();

  scan_spec.row_limit = 1;
  scan_spec.max_versions = 1;
  ri.start = m_metadata_key.c_str();
  ri.end   = m_metadata_key.c_str();
  scan_spec.row_intervals.push_back(ri);
  scan_spec.columns.clear();
  scan_spec.columns.push_back("Files");
  scan_spec.columns.push_back("NextCSID");

  TableScannerPtr files_scanner = Global::metadata_table->create_scanner(scan_spec);

  String ag_name;

  while (files_scanner->next(cell)) {
    if (!strcmp(cell.column_family, "Files")) {
      ag_name = String(cell.column_qualifier);
      m_ag_map[ag_name].files = String((const char *)cell.value, cell.value_len);
    }
    else if (!strcmp(cell.column_family, "NextCSID")) {
      ag_name = String(cell.column_qualifier);
      String idstr = String((const char *)cell.value, cell.value_len);
      m_ag_map[ag_name].nextcsid = atoi(idstr.c_str());
    }
  }

  m_ag_map_iter = m_ag_map.begin();

  files_scanner = 0;

}



bool MetadataNormal::get_next_files(String &ag_name, String &files, uint32_t *nextcsidp) {

  if (m_ag_map_iter == m_ag_map.end())
    return false;

  ag_name = m_ag_map_iter->first;
  files = m_ag_map_iter->second.files;
  *nextcsidp = m_ag_map_iter->second.nextcsid;

  m_ag_map_iter++;
  return true;
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


void MetadataNormal::write_files(const String &ag_name, const String &files, uint32_t nextcsid) {
  TableMutatorPtr mutator;
  KeySpec key;

  mutator = Global::metadata_table->create_mutator();

  key.row = m_metadata_key.c_str();
  key.row_len = m_metadata_key.length();
  key.column_family = "Files";
  key.column_qualifier = ag_name.c_str();
  key.column_qualifier_len = ag_name.length();
  mutator->set(key, (uint8_t *)files.c_str(), files.length());

  char buf[32];
  sprintf(buf, "%u", (unsigned)nextcsid);
  key.row = m_metadata_key.c_str();
  key.row_len = m_metadata_key.length();
  key.column_family = "NextCSID";
  key.column_qualifier = ag_name.c_str();
  key.column_qualifier_len = ag_name.length();
  mutator->set(key, (uint8_t *)buf, strlen(buf));

  mutator->flush();
}
