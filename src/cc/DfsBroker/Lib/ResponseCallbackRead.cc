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

#include "AsyncComm/CommBuf.h"

#include "ResponseCallbackRead.h"

using namespace hypertable;
using namespace hypertable::DfsBroker;

int ResponseCallbackRead::response(uint64_t offset, uint32_t nread, uint8_t *data) {
  CommBufPtr cbufPtr( new CommBuf(hbuilder_.HeaderLength() + 16 ) );
  cbufPtr->PrependInt(nread);
  cbufPtr->PrependLong(offset);
  cbufPtr->PrependInt(Error::OK);

  if (nread > 0)
    cbufPtr->SetExt(data, nread);

  hbuilder_.LoadFromMessage(mEventPtr->header);
  hbuilder_.Encapsulate(cbufPtr.get());
  return mComm->SendResponse(mEventPtr->addr, cbufPtr);
}
