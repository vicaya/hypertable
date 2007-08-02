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

#include "ResponseCallbackFetchScanblock.h"

using namespace hypertable;

int ResponseCallbackFetchScanblock::response(short moreFlag, int32_t id, ExtBufferT &ext) {
  CommBufPtr cbufPtr( new CommBuf(hbuilder_.HeaderLength() + 10) );
  cbufPtr->SetExt(ext.buf, ext.len);
  cbufPtr->PrependInt(id);   // scanner ID
  cbufPtr->PrependShort(moreFlag);
  cbufPtr->PrependInt(Error::OK);
  hbuilder_.LoadFromMessage(mEventPtr->header);
  hbuilder_.Encapsulate(cbufPtr.get());
  return mComm->SendResponse(mEventPtr->addr, cbufPtr);
}

