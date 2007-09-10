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

using namespace hypertable;

/**
 * 
 */
MergeScanner::MergeScanner(ScanContextPtr &scanContextPtr, bool returnDeletes) : CellListScanner(scanContextPtr), mInitialized(false), mScanners(), mQueue(), mDeletePresent(false), mDeletedRow(0), mDeletedCell(0), mReturnDeletes(returnDeletes) {
}



void MergeScanner::AddScanner(CellListScanner *scanner) {
  mScanners.push_back(scanner);
}



MergeScanner::~MergeScanner() {
  for (size_t i=0; i<mScanners.size(); i++)
    delete mScanners[i];
}



void MergeScanner::Forward() {
  ScannerStateT sstate;
  Key keyComps;
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

    if (!keyComps.load(sstate.key)) {
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
      if (mReturnDeletes)
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
      if (mReturnDeletes)
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

  if (!mInitialized)
    Initialize();

  if (!mQueue.empty()) {
    const ScannerStateT &sstate = mQueue.top();
    *keyp = sstate.key;
    *valuep = sstate.value;
    return true;
  }
  return false;
}



void MergeScanner::Initialize() {
  ScannerStateT sstate;
  while (!mQueue.empty())
    mQueue.pop();
  for (size_t i=0; i<mScanners.size(); i++) {
    if (mScanners[i]->Get(&sstate.key, &sstate.value)) {
      sstate.scanner = mScanners[i];
      mQueue.push(sstate);
    }
  }
  if (!mQueue.empty()) {
    const ScannerStateT &sstate = mQueue.top();
    Key keyComps;
    if (!keyComps.load(sstate.key)) {
      assert(!"MergeScanner::Initialize() - Problem decoding key!");
    }

    if (keyComps.flag == FLAG_DELETE_ROW) {
      size_t len = strlen(keyComps.rowKey) + 1;
      mDeletedRow.clear();
      mDeletedRow.ensure(len);
      memcpy(mDeletedRow.buf, keyComps.rowKey, len);
      mDeletedRow.ptr = mDeletedRow.buf + len;
      mDeletedRowTimestamp = keyComps.timestamp;
      mDeletePresent = true;
      if (!mReturnDeletes)
	Forward();
    }
    else if (keyComps.flag == FLAG_DELETE_CELL) {
      size_t len = (keyComps.columnQualifier - keyComps.rowKey) + strlen(keyComps.columnQualifier) + 1;
      mDeletedCell.clear();
      mDeletedCell.ensure(len);
      memcpy(mDeletedCell.buf, sstate.key->data, len);
      mDeletedCell.ptr = mDeletedCell.buf + len;
      mDeletedCellTimestamp = keyComps.timestamp;
      mDeletePresent = true;
      if (!mReturnDeletes)
	Forward();
    }
    else
      mDeletePresent = false;
  }
  mInitialized = true;
}

