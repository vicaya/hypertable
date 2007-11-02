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

#include "ClientBufferedReaderHandler.h"
#include "Client.h"

using namespace hypertable;

namespace {
  const uint32_t DEFAULT_READ_SIZE = 8192;
}

/**
 *
 */
ClientBufferedReaderHandler::ClientBufferedReaderHandler(DfsBroker::Client *client, uint32_t fd, uint32_t bufSize) : mClient(client), mFd(fd), mEof(false), mError(Error::OK) {

  mMaxOutstanding = (bufSize + (DEFAULT_READ_SIZE-1)) / DEFAULT_READ_SIZE;

  if (mMaxOutstanding < 2)
    mMaxOutstanding = 2;

  for (mOutstanding=0; mOutstanding<mMaxOutstanding; mOutstanding++) {
    if ((mError = mClient->Read(mFd, DEFAULT_READ_SIZE, this)) != Error::OK) {
      mEof = true;
      break;
    }
  }

  mPtr = mEndPtr = 0;
}


ClientBufferedReaderHandler::~ClientBufferedReaderHandler() {
  boost::mutex::scoped_lock lock(mMutex);
  mEof = true;
  while (mOutstanding > 0)
    mCond.wait(lock);
}


/**
 *
 */
void ClientBufferedReaderHandler::handle(EventPtr &eventPtr) {
  boost::mutex::scoped_lock lock(mMutex);

  mOutstanding--;

  if (eventPtr->type == Event::MESSAGE) {
    if ((mError = (int)Protocol::ResponseCode(eventPtr)) != Error::OK) {
      LOG_VA_ERROR("DfsBroker 'read' error (amount=%d, fd=%d) : %s",
		   DEFAULT_READ_SIZE, mFd, Protocol::StringFormatMessage(eventPtr).c_str());
      mEof = true;
      return;
    }
    mQueue.push(eventPtr);
    DfsBroker::Protocol::ResponseHeaderReadT *readHeader = (DfsBroker::Protocol::ResponseHeaderReadT *)eventPtr->message;
    if (readHeader->amount < DEFAULT_READ_SIZE)
      mEof = true;
  }
  else if (eventPtr->type == Event::ERROR) {
    LOG_VA_ERROR("%s", eventPtr->toString().c_str());    
    mError = eventPtr->error;
    mEof = true;
  }
  else {
    LOG_VA_ERROR("%s", eventPtr->toString().c_str());
    assert(!"Not supposed to receive this type of event!");
  }

  mCond.notify_all();
}



/**
 *
 */
int ClientBufferedReaderHandler::Read(uint8_t *buf, uint32_t len, uint32_t *nreadp) {
  boost::mutex::scoped_lock lock(mMutex);
  uint8_t *ptr = buf;
  uint32_t nleft = len;
  uint32_t available;

  while (true) {

    while (mQueue.empty() && !mEof)
      mCond.wait(lock);

    if (mError != Error::OK || mQueue.empty())
      return mError;

    if (mPtr == 0) {
      EventPtr &eventPtr = mQueue.front();
      DfsBroker::Protocol::ResponseHeaderReadT *readHeader = (DfsBroker::Protocol::ResponseHeaderReadT *)eventPtr->message;
      mPtr = (uint8_t *)&readHeader[1];
      mEndPtr = mPtr + readHeader->amount;
    }

    available = mEndPtr-mPtr;

    if (available >= nleft) {
      memcpy(ptr, mPtr, nleft);
      *nreadp = len;
      mPtr += nleft;
      if ((mEndPtr-mPtr) == 0) {
	mQueue.pop();
	mPtr = 0;
	ReadAhead();
      }
      return Error::OK;
    }

    memcpy(ptr, mPtr, available);
    ptr += available;
    nleft -= available;
    mQueue.pop();
    mPtr = 0;
    ReadAhead();
  }
}



/**
 *
 */
void ClientBufferedReaderHandler::ReadAhead() {
  uint32_t n = mMaxOutstanding - (mOutstanding + mQueue.size());

  assert(mMaxOutstanding >= (mOutstanding + mQueue.size()));
  
  if (mEof)
    return;

  for (uint32_t i=0; i<n; i++) {
    if ((mError = mClient->Read(mFd, DEFAULT_READ_SIZE, this)) != Error::OK) {
      mEof = true;
      break;
    }
    mOutstanding++;
  }
}


