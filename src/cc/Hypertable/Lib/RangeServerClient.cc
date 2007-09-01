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
#include "AsyncComm/DispatchHandlerSynchronizer.h"

#include "RangeServerClient.h"

using namespace hypertable;


RangeServerClient::RangeServerClient(ConnectionManager *connManager) : mConnectionManager(connManager) {
  mComm = mConnectionManager->GetComm();
}


RangeServerClient::~RangeServerClient() {
  // TODO:  implement me!
  delete mConnectionManager;
  return;
}


int RangeServerClient::LoadRange(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::CreateRequestLoadRange(addr, rangeSpec) );
  return SendMessage(addr, cbufPtr, handler);
}

int RangeServerClient::LoadRange(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::CreateRequestLoadRange(addr, rangeSpec) );
  int error = SendMessage(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("RangeServer 'load range' error : %s", Protocol::StringFormatMessage(eventPtr).c_str());
      error = (int)Protocol::ResponseCode(eventPtr);
    }
  }
  return error;
}

int RangeServerClient::Update(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, uint8_t *data, size_t len, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::CreateRequestUpdate(addr, rangeSpec, data, len) );
  return SendMessage(addr, cbufPtr, handler);
}


int RangeServerClient::Update(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, uint8_t *data, size_t len) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::CreateRequestUpdate(addr, rangeSpec, data, len) );
  int error = SendMessage(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("RangeServer 'update' error : %s", Protocol::StringFormatMessage(eventPtr).c_str());
      error = (int)Protocol::ResponseCode(eventPtr);
    }
  }
  return error;
}


int RangeServerClient::CreateScanner(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, ScanSpecificationT &scanSpec, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::CreateRequestCreateScanner(addr, rangeSpec, scanSpec) );
  return SendMessage(addr, cbufPtr, handler);
}


int RangeServerClient::CreateScanner(struct sockaddr_in &addr, RangeSpecificationT &rangeSpec, ScanSpecificationT &scanSpec) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::CreateRequestCreateScanner(addr, rangeSpec, scanSpec) );
  int error = SendMessage(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("RangeServer 'create scanner' error : %s", Protocol::StringFormatMessage(eventPtr).c_str());
      error = (int)Protocol::ResponseCode(eventPtr);
    }
  }
  return error;
}


int RangeServerClient::FetchScanblock(struct sockaddr_in &addr, int scannerId, DispatchHandler *handler) {
  CommBufPtr cbufPtr( RangeServerProtocol::CreateRequestFetchScanblock(addr, scannerId) );
  return SendMessage(addr, cbufPtr, handler);
}


int RangeServerClient::FetchScanblock(struct sockaddr_in &addr, int scannerId) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( RangeServerProtocol::CreateRequestFetchScanblock(addr, scannerId) );
  int error = SendMessage(addr, cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("RangeServer 'fetch scanblock' error : %s", Protocol::StringFormatMessage(eventPtr).c_str());
      error = (int)Protocol::ResponseCode(eventPtr);
    }
  }
  return error;
}

int RangeServerClient::SendMessage(struct sockaddr_in &addr, CommBufPtr &cbufPtr, DispatchHandler *handler) {
  int error;

  if ((error = mComm->SendRequest(addr, cbufPtr, handler)) != Error::OK) {
    LOG_VA_WARN("Comm::SendRequest to %s:%d failed - %s",
		inet_ntoa(addr.sin_addr), ntohs(addr.sin_port), Error::GetText(error));
  }
  return error;
}
