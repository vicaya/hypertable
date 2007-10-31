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
#include <cstring>

#include "Common/Error.h"

#include "Hyperspace/Session.h"

#include "RootFileHandler.h"
#include "RangeLocator.h"


/**
 * 
 */
RangeLocator::RangeLocator(ConnectionManager *connManager, Hyperspace::Session *hyperspace) : mConnManager(connManager), mHyperspace(hyperspace), mCache(1000), mRootStale(true), mRangeServer(connManager->GetComm(), 30) {
  int error;
  
  mRootHandlerPtr = new RootFileHandler(this);

  if ((error = mHyperspace->Open("/hypertable/root", OPEN_FLAG_READ, mRootHandlerPtr, &mRootFileHandle)) != Error::OK) {
    LOG_VA_ERROR("Unable to open Hyperspace file '/hypertable/root' (%s)", Error::GetText(error));
    throw Exception(error);
  }

}

RangeLocator::~RangeLocator() {
  mHyperspace->Close(mRootFileHandle);
}



/**
 *
 */
int RangeLocator::Find(uint32_t tableId, const char *rowKey, const char **serverIdPtr) {
  int error;

  if (mRootStale) {
    if ((error = ReadRootLocation()) != Error::OK)
      return error;
  }

  if (mCache.Lookup(tableId, rowKey, serverIdPtr))
    return Error::OK;

  // fix me !!!!

  if (tableId == 0) {
    assert(strcmp(rowKey, "0:"));

    /**
    mRangeServer;
    RangeServerClient    
    */
  }

  return Error::OK;
}


/**
 * 
 */
int RangeLocator::ReadRootLocation() {
  int error;
  DynamicBuffer value(0);
  std::string addrStr;
  char *ptr;

  if ((error = mHyperspace->AttrGet(mRootFileHandle, "location", value)) != Error::OK) {
    LOG_VA_ERROR("Problem reading 'location' attribute of Hyperspace file /hypertable/root - %s", Error::GetText(error));
    return error;
  }

  mCache.Insert(0, 0, "1:", (const char *)value.buf);

  if ((ptr = strchr((const char *)value.buf, '_')) != 0)
    *ptr = 0;

  if (!InetAddr::Initialize(&mRootAddr, (const char *)value.buf)) {
    LOG_ERROR("Unable to extract address from /hypertable/root Hyperspace file");
    return Error::BAD_ROOT_SERVERID;
  }

  mConnManager->Add(mRootAddr, 30, "Root RangeServer");

  if (!mConnManager->WaitForConnection(mRootAddr, 20)) {
    LOG_VA_ERROR("Timeout (20s) waiting for root RangeServer connection - %s", (const char *)value.buf);
  }

  mRootStale = false;

  return Error::OK;
}
