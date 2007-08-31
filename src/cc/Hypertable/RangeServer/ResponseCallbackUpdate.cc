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

#include "ResponseCallbackUpdate.h"

using namespace hypertable;

int ResponseCallbackUpdate::response(ExtBufferT &ext) {
  hbuilder_.LoadFromMessage(mEventPtr->header);
  CommBufPtr cbufPtr( new CommBuf(hbuilder_, 4, ext.buf, ext.len) );
  cbufPtr->AppendInt(Error::RANGESERVER_PARTIAL_UPDATE);
  return mComm->SendResponse(mEventPtr->addr, cbufPtr);
}
