/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <cassert>

#include "Common/Logger.h"

#include "Key.h"
#include "MergeScanner.h"

using namespace hypertable;

/**
 * 
 */
MergeScanner::MergeScanner(bool suppressDeleted) : CellListScanner(), mScanners(), mQueue(), mDeletePresent(false), mDeletedRow(0), mDeletedCell(0), mSuppressDeleted(suppressDeleted) {
}

void MergeScanner::AddScanner(CellListScanner *scanner) {
  mScanners.push_back(scanner);
}



MergeScanner::~MergeScanner() {
  for (size_t i=0; i<mScanners.size(); i++)
    delete mScanners[i];
}



void MergeScanner::RestrictRange(KeyT *start, KeyT *end) {
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

  if (mQueue.empty())
    return;

  sstate = mQueue.top();

  /**
   * Pop the top element, forward it, re-insert it back into the queue;
   */
  if (mSuppressDeleted) {
    size_t len;

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
  else {
    mQueue.pop();
    sstate.scanner->Forward();
    if (sstate.scanner->Get(&sstate.key, &sstate.value))
      mQueue.push(sstate);
  }
}

bool MergeScanner::Get(KeyT **keyp, ByteString32T **valuep) {
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
