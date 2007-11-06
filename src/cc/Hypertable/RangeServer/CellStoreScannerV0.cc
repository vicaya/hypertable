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

#include "Common/Error.h"
#include "Common/System.h"

#include "BlockInflaterZlib.h"
#include "Global.h"
#include "CellStoreScannerV0.h"

using namespace hypertable;

CellStoreScannerV0::CellStoreScannerV0(CellStorePtr &cellStorePtr, ScanContextPtr &scanContextPtr) : CellListScanner(scanContextPtr), m_cell_store_ptr(cellStorePtr), m_cur_key(0), m_cur_value(0), m_check_for_range_end(false), m_end_key_ptr() {
  ByteString32T  *tmpKey, *startKey, *endKey;

  m_cell_store_v0 = dynamic_cast< CellStoreV0*>(m_cell_store_ptr.get());
  assert(m_cell_store_v0);

  m_index = m_cell_store_v0->m_index;
  m_file_id = m_cell_store_v0->m_file_id;
  m_block_inflater = new BlockInflaterZlib();
  memset(&m_block, 0, sizeof(m_block));

  // compute startKey
  tmpKey = m_cell_store_v0->m_start_key_ptr.get();
  if (scanContextPtr->startKeyPtr)
    startKey = (tmpKey != 0 && *scanContextPtr->startKeyPtr.get() < *tmpKey) ? tmpKey : scanContextPtr->startKeyPtr.get();
  else
    startKey = tmpKey;

  // compute m_end_key_ptr
  if (scanContextPtr->endKeyPtr) {
    if (!m_cell_store_v0->m_end_key_ptr || *scanContextPtr->endKeyPtr.get() < *m_cell_store_v0->m_end_key_ptr.get())
      m_end_key_ptr = scanContextPtr->endKeyPtr;
    else
      m_end_key_ptr = m_cell_store_v0->m_end_key_ptr;
  }
  else
    m_end_key_ptr = m_cell_store_v0->m_end_key_ptr;

  if (m_block.base != 0)
    Global::blockCache->checkin(m_file_id, m_block.offset);
  memset(&m_block, 0, sizeof(m_block));

  m_check_for_range_end = false;

  if (startKey != 0)
    m_iter = m_index.lower_bound(startKey);
  else
    m_iter = m_index.begin();

  m_cur_key = 0;

  if (!fetch_next_block()) {
    m_iter = m_index.end();
    return;
  }

  /**
   * Seek to start of range in block
   */
  m_cur_key = (ByteString32T *)m_block.ptr;
  m_cur_value = (ByteString32T *)(m_block.ptr + Length(m_cur_key));
  if (startKey != 0) {
    while (*m_cur_key < *startKey) {
      m_block.ptr = ((uint8_t *)m_cur_value) + Length(m_cur_value);
      if (m_block.ptr >= m_block.end) {
	m_iter = m_index.end();
	return;
      }
      m_cur_key = (ByteString32T *)m_block.ptr;
      m_cur_value = (ByteString32T *)(m_block.ptr + Length(m_cur_key));
    }
  }

  /**
   * End of range check
   */
  if (m_check_for_range_end) {
    if (!(*m_cur_key < *m_end_key_ptr.get())) {
      m_iter = m_index.end();
      return;
    }
  }

}


CellStoreScannerV0::~CellStoreScannerV0() {
  if (m_block.base != 0)
    Global::blockCache->checkin(m_file_id, m_block.offset);
  delete m_block_inflater;
}


bool CellStoreScannerV0::get(ByteString32T **keyp, ByteString32T **valuep) {

  if (m_iter == m_index.end())
    return false;

  *keyp = m_cur_key;
  *valuep = m_cur_value;

  return true;
}



void CellStoreScannerV0::forward() {
  Key keyComps;

  while (true) {

    if (m_iter == m_index.end())
      return;

    m_block.ptr = ((uint8_t *)m_cur_value) + Length(m_cur_value);    

    if (m_block.ptr >= m_block.end) {
      if (!fetch_next_block())
	return;
    }

    m_cur_key = (ByteString32T *)m_block.ptr;
    m_cur_value = (ByteString32T *)(m_block.ptr + Length(m_cur_key));

    if (m_check_for_range_end) {
      if (!(*m_cur_key < *m_end_key_ptr.get())) {
	m_iter = m_index.end();
	break;
      }
    }

    /**
     * Column family check
     */
    if (!keyComps.load(m_cur_key)) {
      LOG_ERROR("Problem parsing key!");
      break;
    }
    if (m_scan_context_ptr->familyMask[keyComps.columnFamily])
      break;
  }
}



/**
 * This method fetches the 'next' compressed block of key/value pairs from
 * the underlying CellStore.
 *
 * Preconditions required to call this method:
 *  1. m_block is cleared and m_iter points to the m_index entry of the first block to fetch
 *    'or'
 *  2. m_block is loaded with the current block and m_iter points to the m_index entry of
 *     the current block
 *
 * @return true if next block successfully fetched, false if no next block
 */
bool CellStoreScannerV0::fetch_next_block() {
  uint8_t *buf = 0;
  bool rval = false;

  // If we're at the end of the current block, deallocate and move to next
  if (m_block.base != 0 && m_block.ptr >= m_block.end) {
    Global::blockCache->checkin(m_file_id, m_block.offset);
    memset(&m_block, 0, sizeof(m_block));
    m_iter++;
  }

  if (m_block.base == 0 && m_iter != m_index.end()) {
    DynamicBuffer expandBuffer(0);
    uint32_t len;

    m_block.offset = (*m_iter).second;

    CellStoreV0::IndexMapT::iterator iterNext = m_iter;
    iterNext++;
    if (iterNext == m_index.end()) {
      m_block.zlength = m_cell_store_v0->m_trailer.fixIndexOffset - m_block.offset;
      if (m_end_key_ptr.get() != 0)
	m_check_for_range_end = true;
    }
    else {
      if (m_end_key_ptr.get() != 0 && !(*((*iterNext).first) < *m_end_key_ptr.get()))
	m_check_for_range_end = true;
      m_block.zlength = (*iterNext).second - m_block.offset;
    }

    /**
     * Cache lookup / block read
     */
    if (!Global::blockCache->checkout(m_file_id, (uint32_t)m_block.offset, &m_block.base, &len)) {
      /** Read compressed block **/
      buf = new uint8_t [ m_block.zlength ];
      uint32_t nread;
      if (m_cell_store_v0->m_filesys->pread(m_cell_store_v0->m_fd, m_block.offset, m_block.zlength, buf, &nread) != Error::OK)
	goto abort;

      /** inflate compressed block **/
      if (!m_block_inflater->inflate(buf, m_block.zlength, Constants::DATA_BLOCK_MAGIC, expandBuffer))
	goto abort;

      /** take ownership of inflate buffer **/
      size_t fill;
      m_block.base = expandBuffer.release(&fill);
      len = fill;

      /** Insert block into cache  **/
      if (!Global::blockCache->insert_and_checkout(m_file_id, (uint32_t)m_block.offset, m_block.base, len)) {
	delete [] m_block.base;
	if (!Global::blockCache->checkout(m_file_id, (uint32_t)m_block.offset, &m_block.base, &len)) {
	  LOG_VA_ERROR("Problem checking out block from cache fileId=%d, offset=%ld", m_file_id, (uint32_t)m_block.offset);
	  DUMP_CORE;
	}
      }
    }
    m_block.ptr = m_block.base;
    m_block.end = m_block.base + len;

    rval = true;
  }

 abort:
  delete [] buf;
  return rval;
}
