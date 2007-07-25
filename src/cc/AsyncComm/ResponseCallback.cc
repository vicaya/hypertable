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

#include "Common/Error.h"

#include "CommBuf.h"
#include "Protocol.h"
#include "ResponseCallback.h"

using namespace hypertable;

int ResponseCallback::error(int error, std::string &msg) {
  CommBuf *cbuf = Protocol::CreateErrorMessage(0, error, msg.c_str(), mBuilder.HeaderLength()); // fix me!!!
  mBuilder.LoadFromMessage(mEventPtr->header);
  mBuilder.Encapsulate(cbuf);
  {
    CommBufPtr cbufPtr(cbuf);
    return mComm->SendResponse(mEventPtr->addr, cbufPtr);
  }
}

int ResponseCallback::response() {
  CommBuf *cbuf = new CommBuf(mBuilder.HeaderLength() + 6);
  cbuf->PrependShort(0); // fix me!!!
  cbuf->PrependInt(Error::OK);
  mBuilder.LoadFromMessage(mEventPtr->header);
  mBuilder.Encapsulate(cbuf);
  {
    CommBufPtr cbufPtr(cbuf);
    return mComm->SendResponse(mEventPtr->addr, cbufPtr);
  }
}
