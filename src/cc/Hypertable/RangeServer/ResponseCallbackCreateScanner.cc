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

#include "ResponseCallbackCreateScanner.h"

using namespace hypertable;

int ResponseCallbackCreateScanner::response(short moreFlag, int32_t id, ExtBufferT &ext) {
  CommBuf *cbuf = new CommBuf(mBuilder.HeaderLength() + 12);
  cbuf->SetExt(ext.buf, ext.len);
  cbuf->PrependInt(id);   // scanner ID
  cbuf->PrependShort(moreFlag);
  cbuf->PrependShort(0); // fix me!!!
  cbuf->PrependInt(Error::OK);
  mBuilder.LoadFromMessage(mEventPtr->header);
  mBuilder.Encapsulate(cbuf);
  {
    CommBufPtr cbufPtr(cbuf);
    return mComm->SendResponse(mEventPtr->addr, cbufPtr);
  }
}
