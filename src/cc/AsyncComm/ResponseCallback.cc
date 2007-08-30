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

#include "Common/Error.h"

#include "CommBuf.h"
#include "Protocol.h"
#include "ResponseCallback.h"

using namespace hypertable;

int ResponseCallback::error(int error, std::string msg) {
  hbuilder_.LoadFromMessage(mEventPtr->header);
  CommBufPtr cbufPtr( Protocol::CreateErrorMessage(hbuilder_, error, msg.c_str()) );
  return mComm->SendResponse(mEventPtr->addr, cbufPtr);
}

int ResponseCallback::response_ok() {
  CommBufPtr cbufPtr( new CommBuf(hbuilder_, 4) );
  cbufPtr->AppendInt(Error::OK);
  return mComm->SendResponse(mEventPtr->addr, cbufPtr);
}

