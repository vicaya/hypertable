/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#include <cassert>
#include <cstring>

#include "Common/Error.h"

#include "Hypertable/Lib/TableMutator.h"
#include "Hypertable/Lib/TableScanner.h"

#include "Global.h"
#include "MetadataNormal.h"


/**
 *
 */
MetadataNormal::MetadataNormal(TableIdentifierT &identifier, std::string &end_row) : m_files_scanner_ptr(0) {
  m_metadata_key = std::string("") + (uint32_t)identifier.id + ":" + end_row;
  memcpy(&m_identifier, &identifier, sizeof(TableIdentifierT));
}



/**
 *
 */
MetadataNormal::~MetadataNormal() {
  m_files_scanner_ptr = 0;
}



void MetadataNormal::reset_files_scan() {
  int error;
  ScanSpecificationT scan_spec;

  scan_spec.rowLimit = 1;
  scan_spec.max_versions = 1;
  scan_spec.startRow = m_metadata_key.c_str();
  scan_spec.startRowInclusive = true;
  scan_spec.endRow = m_metadata_key.c_str();
  scan_spec.endRowInclusive = true;
  scan_spec.interval.first = 0;
  scan_spec.interval.second = 0;
  scan_spec.columns.clear();
  scan_spec.columns.push_back("Files");
  scan_spec.return_deletes = false;

  if ((error = Global::metadata_table_ptr->create_scanner(scan_spec, m_files_scanner_ptr)) != Error::OK)
    throw Hypertable::Exception(error, "Problem creating scanner on METADATA table");
}



bool MetadataNormal::get_next_files(std::string &ag_name, std::string &files) {
  CellT cell;

  assert(m_files_scanner_ptr);

  if (m_files_scanner_ptr->next(cell)) {
    assert(!strcmp(cell.column_family, "Files"));
    ag_name = std::string(cell.column_qualifier);
    files = std::string((const char *)cell.value, cell.value_len);
    return true;
  }

  m_files_scanner_ptr = 0;
  return false;
}



void MetadataNormal::write_files(std::string &ag_name, std::string &files) {
  int error;
  TableMutatorPtr mutator_ptr;
  KeySpec key;

  if ((error = Global::metadata_table_ptr->create_mutator(mutator_ptr)) != Error::OK)
    throw Hypertable::Exception(error, "Problem creating mutator on METADATA table");

  key.row = m_metadata_key.c_str();
  key.row_len = m_metadata_key.length();
  key.column_family = "Files";
  key.column_qualifier = ag_name.c_str();
  key.column_qualifier_len = ag_name.length();
  mutator_ptr->set(0, key, (uint8_t *)files.c_str(), files.length());
  mutator_ptr->flush();

}
