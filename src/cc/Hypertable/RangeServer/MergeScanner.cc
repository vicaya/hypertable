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

#include <cassert>

#include "Common/Logger.h"

#include "Hypertable/Lib/Key.h"

#include "MergeScanner.h"

using namespace Hypertable;

/**
 * 
 */
MergeScanner::MergeScanner(ScanContextPtr &scanContextPtr, bool returnDeletes) : CellListScanner(scanContextPtr), m_done(false), m_initialized(false), m_scanners(), m_queue(), m_delete_present(false), m_deleted_row(0), m_deleted_cell(0), m_return_deletes(returnDeletes), m_row_count(0), m_row_limit(0), m_cell_count(0), m_cell_limit(0), m_cell_cutoff(0), m_prev_key(0) {
  if (scanContextPtr->spec != 0)
    m_row_limit = scanContextPtr->spec->rowLimit;
  m_start_timestamp = scanContextPtr->interval.first;
  m_end_timestamp = scanContextPtr->interval.second;
}



void MergeScanner::add_scanner(CellListScanner *scanner) {
  m_scanners.push_back(scanner);
}



MergeScanner::~MergeScanner() {
  for (size_t i=0; i<m_scanners.size(); i++)
    delete m_scanners[i];
}



void MergeScanner::forward() {
  ScannerStateT sstate;
  Key keyComps;
  size_t len;

  if (m_queue.empty())
    return;

  sstate = m_queue.top();

  /**
   * Pop the top element, forward it, re-insert it back into the queue;
   */

  while (true) {

    while (true) {

      m_queue.pop();
      sstate.scanner->forward();
      if (sstate.scanner->get(&sstate.key, &sstate.value))
	m_queue.push(sstate);

      if (m_queue.empty())
	return;

      sstate = m_queue.top();

      if (!keyComps.load(sstate.key)) {
	LOG_ERROR("Problem decoding key!");
      }
      else if (keyComps.timestamp < m_start_timestamp) {
	continue;
      }
      else if (keyComps.flag == FLAG_DELETE_ROW) {
	len = strlen(keyComps.row) + 1;
	if (m_delete_present && m_deleted_row.fill() == len && !memcmp(m_deleted_row.buf, keyComps.row, len)) {
	  if (m_deleted_row_timestamp < keyComps.timestamp)
	    m_deleted_row_timestamp = keyComps.timestamp;
	}
	else {
	  m_deleted_row.clear();
	  m_deleted_row.ensure(len);
	  memcpy(m_deleted_row.buf, keyComps.row, len);
	  m_deleted_row.ptr = m_deleted_row.buf + len;
	  m_deleted_row_timestamp = keyComps.timestamp;
	  m_delete_present = true;
	}
	if (m_return_deletes)
	  break;
      }
      else if (keyComps.flag == FLAG_DELETE_CELL) {
	len = (keyComps.column_qualifier - keyComps.row) + strlen(keyComps.column_qualifier) + 1;
	if (m_delete_present && m_deleted_cell.fill() == len && !memcmp(m_deleted_cell.buf, keyComps.row, len)) {
	  if (m_deleted_cell_timestamp < keyComps.timestamp)
	    m_deleted_cell_timestamp = keyComps.timestamp;
	}
	else {
	  m_deleted_cell.clear();
	  m_deleted_cell.ensure(len);
	  memcpy(m_deleted_cell.buf, sstate.key->data, len);
	  m_deleted_cell.ptr = m_deleted_cell.buf + len;
	  m_deleted_cell_timestamp = keyComps.timestamp;
	  m_delete_present = true;
	}
	if (m_return_deletes)
	  break;
      }
      else {
	if (keyComps.timestamp >= m_end_timestamp)
	  continue;
	if (m_delete_present) {
	  if (m_deleted_cell.fill() > 0) {
	    len = (keyComps.column_qualifier - keyComps.row) + strlen(keyComps.column_qualifier) + 1;
	    if (m_deleted_cell.fill() == len && !memcmp(m_deleted_cell.buf, keyComps.row, len)) {
	      if (keyComps.timestamp < m_deleted_cell_timestamp)
		continue;
	      break;
	    }
	    m_deleted_cell.clear();
	  }
	  if (m_deleted_row.fill() > 0) {
	    len = strlen(keyComps.row) + 1;
	    if (m_deleted_row.fill() == len && !memcmp(m_deleted_row.buf, keyComps.row, len)) {
	      if (keyComps.timestamp < m_deleted_row_timestamp)
		continue;
	      break;
	    }
	    m_deleted_row.clear();
	  }
	  m_delete_present = false;
	}
	break;
      }
    }

    if (m_prev_key.fill() != 0) {

      if (m_row_limit) {
	if (strcmp(keyComps.row, (const char *)m_prev_key.buf)) {
	  m_row_count++;
	  if (m_row_count >= m_row_limit) {
	    m_done = true;
	    return;
	  }
	  m_prev_key.set(sstate.key->data, sstate.key->len);
	  m_cell_limit = m_scan_context_ptr->familyInfo[keyComps.column_family_code].max_versions;
	  m_cell_cutoff = m_scan_context_ptr->familyInfo[keyComps.column_family_code].cutoffTime;
	  m_cell_count = 0;
	  return;
	}
      }

      if (sstate.key->len == m_prev_key.fill() && sstate.key->len > 9 &&
	  !memcmp(sstate.key->data, m_prev_key.buf, sstate.key->len-9)) {
	if (m_cell_limit) {
	  m_cell_count++;
	  m_prev_key.set(sstate.key->data, sstate.key->len);
	  if (m_cell_count >= m_cell_limit)
	    continue;
	}
      }
      else {
	m_prev_key.set(sstate.key->data, sstate.key->len);
	m_cell_limit = m_scan_context_ptr->familyInfo[keyComps.column_family_code].max_versions;
	m_cell_cutoff = m_scan_context_ptr->familyInfo[keyComps.column_family_code].cutoffTime;
	m_cell_count = 0;
      }

    }
    else {
      m_prev_key.set(sstate.key->data, sstate.key->len);
      m_cell_limit = m_scan_context_ptr->familyInfo[keyComps.column_family_code].max_versions;
      m_cell_cutoff = m_scan_context_ptr->familyInfo[keyComps.column_family_code].cutoffTime;
      m_cell_count = 0;
    }

    break;
  }
  

}



bool MergeScanner::get(ByteString32T **keyp, ByteString32T **valuep) {

  if (!m_initialized)
    initialize();

  if (!m_queue.empty() && !m_done) {
    const ScannerStateT &sstate = m_queue.top();
    // check for row or cell limit
    *keyp = sstate.key;
    *valuep = sstate.value;
    return true;
  }
  return false;
}



void MergeScanner::initialize() {
  ScannerStateT sstate;
  while (!m_queue.empty())
    m_queue.pop();
  for (size_t i=0; i<m_scanners.size(); i++) {
    if (m_scanners[i]->get(&sstate.key, &sstate.value)) {
      sstate.scanner = m_scanners[i];
      m_queue.push(sstate);
    }
  }
  while (!m_queue.empty()) {
    const ScannerStateT &sstate = m_queue.top();
    Key keyComps;
    if (!keyComps.load(sstate.key)) {
      assert(!"MergeScanner::initialize() - Problem decoding key!");
    }

    if (keyComps.timestamp < m_start_timestamp) {
      ScannerStateT tmp_sstate;
      m_queue.pop();
      sstate.scanner->forward();
      tmp_sstate.scanner = sstate.scanner;
      if (sstate.scanner->get(&tmp_sstate.key, &tmp_sstate.value))
	m_queue.push(tmp_sstate);
      continue;
    }

    if (keyComps.flag == FLAG_DELETE_ROW) {
      size_t len = strlen(keyComps.row) + 1;
      m_deleted_row.clear();
      m_deleted_row.ensure(len);
      memcpy(m_deleted_row.buf, keyComps.row, len);
      m_deleted_row.ptr = m_deleted_row.buf + len;
      m_deleted_row_timestamp = keyComps.timestamp;
      m_delete_present = true;
      if (!m_return_deletes)
	forward();
    }
    else if (keyComps.flag == FLAG_DELETE_CELL) {
      size_t len = (keyComps.column_qualifier - keyComps.row) + strlen(keyComps.column_qualifier) + 1;
      m_deleted_cell.clear();
      m_deleted_cell.ensure(len);
      memcpy(m_deleted_cell.buf, sstate.key->data, len);
      m_deleted_cell.ptr = m_deleted_cell.buf + len;
      m_deleted_cell_timestamp = keyComps.timestamp;
      m_delete_present = true;
      if (!m_return_deletes)
	forward();
    }
    else {
      if (keyComps.timestamp >= m_end_timestamp) {
	ScannerStateT tmp_sstate;
	m_queue.pop();
	sstate.scanner->forward();
	tmp_sstate.scanner = sstate.scanner;
	if (sstate.scanner->get(&tmp_sstate.key, &tmp_sstate.value))
	  m_queue.push(tmp_sstate);
	continue;
      }
      m_delete_present = false;
      m_prev_key.set(sstate.key->data, sstate.key->len);
      m_cell_limit = m_scan_context_ptr->familyInfo[keyComps.column_family_code].max_versions;
      m_cell_cutoff = m_scan_context_ptr->familyInfo[keyComps.column_family_code].cutoffTime;
      m_cell_count = 0;
    }
    break;
  }

  m_initialized = true;
}

