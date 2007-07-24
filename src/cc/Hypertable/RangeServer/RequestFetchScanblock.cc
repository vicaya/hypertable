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
#include <iostream>

#include <boost/shared_array.hpp>

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/StringExt.h"

#include "AsyncComm/Event.h"
#include "AsyncComm/MessageBuilderSimple.h"

#include "FillScanBlock.h"
#include "Global.h"
#include "CellListScanner.h"
#include "Range.h"
#include "RequestFetchScanblock.h"

using namespace hypertable;
using namespace hypertable;
using namespace std;

namespace {
  const int DEFAULT_SCANBUF_SIZE = 32768;
}

/**
 *
 * NOTE: Add start row to request
 *
 *  1. Make sure we have up-to-date table information (table Id, schema, etc.), if not, reload
 *  2. Read Range info from the METADATA table (report split if detected)
 *  3. If we're the root range, then the end row will be "0 0 " so we either read the
 *     Range info from a well-known location in Chubby, or just 'readdir' in the
 *     Range directory /hypertable/tables/METADATA/...
 *  4. Create Range by passing to the constructor the following:
 *    - Schema
 *    - Range files
 *  6. Return
 */
void RequestFetchScanblock::run() {
  size_t remaining = mEvent.messageLen - sizeof(int16_t);
  uint8_t *msgPtr = mEvent.message + sizeof(int16_t);
  string errMsg;
  int error = Error::OK;
  CommBuf *cbuf;
  MessageBuilderSimple mbuilder;
  uint32_t scannerId;
  CellListScannerPtr scannerPtr;
  RangePtr rangePtr;
  bool more = true;
  uint8_t *kvBuffer = 0;
  uint32_t *kvLenp = 0;
  uint32_t bytesFetched = 0;

  /**
   * Decode parameters
   */

  /** Scanner ID **/
  if (remaining < sizeof(int32_t)) {
    error = Error::PROTOCOL_ERROR;
    errMsg = "Unable to decode scanner Id, request truncated";
    goto abort;
  }
  memcpy(&scannerId, msgPtr, sizeof(int32_t));

  if (!Global::scannerMap.Get(scannerId, scannerPtr, rangePtr)) {
    error = Error::RANGESERVER_INVALID_SCANNER_ID;
    char tbuf[32];
    sprintf(tbuf, "%d", scannerId);
    errMsg = (string)tbuf;
    goto abort;
    
  }

  if (Global::verbose)
    cout << "Scanner ID = " << scannerId << endl;

  kvBuffer = new uint8_t [ sizeof(int32_t) + DEFAULT_SCANBUF_SIZE ];
  kvLenp = (uint32_t *)kvBuffer;

  rangePtr->LockShareable();
  more = FillScanBlock(scannerPtr, kvBuffer+sizeof(int32_t), DEFAULT_SCANBUF_SIZE, kvLenp);
  if (!more)
    Global::scannerMap.Remove(scannerId);
  rangePtr->UnlockShareable();
  bytesFetched = *kvLenp;

  /**
   *  Send back data
   */

  cbuf = new CommBuf(mbuilder.HeaderLength() + 12);
  cbuf->SetExt(kvBuffer, sizeof(int32_t) + *kvLenp);
  cbuf->PrependInt(scannerId);
  if (more)
    cbuf->PrependShort(0);
  else
    cbuf->PrependShort(1);
  cbuf->PrependShort(RangeServerProtocol::COMMAND_FETCH_SCANBLOCK);
  cbuf->PrependInt(Error::OK);
  mbuilder.LoadFromMessage(mEvent.header);
  mbuilder.Encapsulate(cbuf);
  if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
    LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
  }

  error = Error::OK;

  if (Global::verbose) {
    LOG_VA_INFO("Successfully fetched %d bytes of scan data", bytesFetched);
  }

 abort:
  if (error != Error::OK) {
    if (Global::verbose) {
      LOG_VA_ERROR("%s '%s'", Error::GetText(error), errMsg.c_str());
    }
    cbuf = Global::protocol->CreateErrorMessage(RangeServerProtocol::COMMAND_FETCH_SCANBLOCK, error,
						errMsg.c_str(), mbuilder.HeaderLength());
    mbuilder.LoadFromMessage(mEvent.header);
    mbuilder.Encapsulate(cbuf);
    if ((error = Global::comm->SendResponse(mEvent.addr, cbuf)) != Error::OK) {
      LOG_VA_ERROR("Comm.SendResponse returned '%s'", Error::GetText(error));
    }
  }
  return;
}

