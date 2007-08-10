/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
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

#include "Key.h"
#include "MergeScanner.h"

using namespace hypertable;

/**
 * 
 */
MergeScanner::MergeScanner(bool showDeletes) : CellListScanner(), mScanners(), mQueue(), mDeletePresent(false), mDeletedRow(0), mDeletedCell(0), mShowDeletes(showDeletes) {
}

void MergeScanner::AddScanner(CellListScanner *scanner) {
  mScanners.push_back(scanner);
}



MergeScanner::~MergeScanner() {
  for (size_t i=0; i<mScanners.size(); i++)
    delete mScanners[i];
}



void MergeScanner::RestrictRange(ByteString32T *start, ByteString32T *end) {
  for (size_t i=0; i<mScanners.size(); i++)
    mScanners[i]->RestrictRange(start, end);
  mReset = false;
}

void MergeScanner::RestrictColumns(const uint8_t *families, size_t count) {
  for (size_t i=0; i<mScanners.size(); i++)
    mScanners[i]->RestrictColumns(families, count);
}


void MergeScanner::Forward() {
  ScannerStateT sstate;
  KeyComponentsT keyComps;
  size_t len;

  if (mQueue.empty())
    return;

  sstate = mQueue.top();

  /**
   * Pop the top element, forward it, re-insert it back into the queue;
   */

  while (true) {

    mQueue.pop();
    sstate.scanner->Forward();
    if (sstate.scanner->Get(&sstate.key, &sstate.value))
      mQueue.push(sstate);

    if (mQueue.empty())
      return;

    sstate = mQueue.top();

    if (!Load(sstate.key, keyComps)) {
      LOG_ERROR("Problem decoding key!");
    }
    else if (keyComps.flag == FLAG_DELETE_ROW) {
      len = strlen(keyComps.rowKey) + 1;
      if (mDeletePresent && mDeletedRow.fill() == len && !memcmp(mDeletedRow.buf, keyComps.rowKey, len)) {
	if (mDeletedRowTimestamp < keyComps.timestamp)
	  mDeletedRowTimestamp = keyComps.timestamp;
      }
      else {
	mDeletedRow.clear();
	mDeletedRow.ensure(len);
	memcpy(mDeletedRow.buf, keyComps.rowKey, len);
	mDeletedRow.ptr = mDeletedRow.buf + len;
	mDeletedRowTimestamp = keyComps.timestamp;
	mDeletePresent = true;
      }
      if (mShowDeletes)
	break;
    }
    else if (keyComps.flag == FLAG_DELETE_CELL) {
      len = (keyComps.columnQualifier - keyComps.rowKey) + strlen(keyComps.columnQualifier) + 1;
      if (mDeletePresent && mDeletedCell.fill() == len && !memcmp(mDeletedCell.buf, keyComps.rowKey, len)) {
	if (mDeletedCellTimestamp < keyComps.timestamp)
	  mDeletedCellTimestamp = keyComps.timestamp;
      }
      else {
	mDeletedCell.clear();
	mDeletedCell.ensure(len);
	memcpy(mDeletedCell.buf, sstate.key->data, len);
	mDeletedCell.ptr = mDeletedCell.buf + len;
	mDeletedCellTimestamp = keyComps.timestamp;
	mDeletePresent = true;
      }
      if (mShowDeletes)
	break;
    }
    else {
      if (mDeletePresent) {
	if (mDeletedCell.fill() > 0) {
	  len = (keyComps.columnQualifier - keyComps.rowKey) + strlen(keyComps.columnQualifier) + 1;
	  if (mDeletedCell.fill() == len && !memcmp(mDeletedCell.buf, keyComps.rowKey, len)) {
	    if (keyComps.timestamp < mDeletedCellTimestamp)
	      continue;
	    break;
	  }
	  mDeletedCell.clear();
	}
	if (mDeletedRow.fill() > 0) {
	  len = strlen(keyComps.rowKey) + 1;
	  if (mDeletedRow.fill() == len && !memcmp(mDeletedRow.buf, keyComps.rowKey, len)) {
	    if (keyComps.timestamp < mDeletedRowTimestamp)
	      continue;
	    break;
	  }
	  mDeletedRow.clear();
	}
	mDeletePresent = false;
      }
      break;
    }
  }

}

bool MergeScanner::Get(ByteString32T **keyp, ByteString32T **valuep) {
  assert(mReset);
  if (!mQueue.empty()) {
    const ScannerStateT &sstate = mQueue.top();
    *keyp = sstate.key;
    *valuep = sstate.value;
    return true;
  }
  return false;
}

void MergeScanner::Reset() {
  ScannerStateT sstate;
  while (!mQueue.empty())
    mQueue.pop();
  for (size_t i=0; i<mScanners.size(); i++) {
    mScanners[i]->Reset();
    if (mScanners[i]->Get(&sstate.key, &sstate.value)) {
      sstate.scanner = mScanners[i];
      mQueue.push(sstate);
    }
  }
  if (!mQueue.empty()) {
    const ScannerStateT &sstate = mQueue.top();
    KeyComponentsT keyComps;
    if (!Load(sstate.key, keyComps)) {
      assert(!"MergeScanner::Reset() - Problem decoding key!");
    }
    mDeletePresent = false;
    if (keyComps.flag == FLAG_DELETE_ROW || keyComps.flag == FLAG_DELETE_CELL)
      Forward();
  }
  mReset = true;
}
