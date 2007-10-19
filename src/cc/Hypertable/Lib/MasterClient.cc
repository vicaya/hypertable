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
#include "AsyncComm/Serialization.h"

#include "MasterClient.h"


MasterClient::MasterClient(Comm *comm, struct sockaddr_in &addr, time_t timeout) : mComm(comm), mAddr(addr), mTimeout(timeout) {
  mProtocol = new MasterProtocol();
}


MasterClient::~MasterClient() {
  delete mProtocol;
}


int MasterClient::CreateTable(const char *tableName, const char *schemaString, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateCreateTableRequest(tableName, schemaString) );
  return SendMessage(cbufPtr, handler, msgIdp);
}



int MasterClient::CreateTable(const char *tableName, const char *schemaString) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateCreateTableRequest(tableName, schemaString) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Master 'create table' error, tableName=%s : %s", tableName, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}


int MasterClient::GetSchema(const char *tableName, DispatchHandler *handler, uint32_t *msgIdp) {
  CommBufPtr cbufPtr( mProtocol->CreateGetSchemaRequest(tableName) );
  return SendMessage(cbufPtr, handler, msgIdp);
}


int MasterClient::GetSchema(const char *tableName, std::string &schema) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateGetSchemaRequest(tableName) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Master 'get schema' error, tableName=%s : %s", tableName, mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
    else {
      uint8_t *ptr = eventPtr->message + sizeof(int32_t);
      size_t remaining = eventPtr->messageLen - sizeof(int32_t);
      const char *schemaStr;
      if (!Serialization::DecodeString(&ptr, &remaining, &schemaStr)) {
	LOG_VA_ERROR("Problem decoding response to 'get schema' command for table '%s'", tableName);
	return Error::PROTOCOL_ERROR;
      }
      schema = schemaStr;
    }
  }
  return error;
}


int MasterClient::Status() {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( mProtocol->CreateStatusRequest() );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      LOG_VA_ERROR("Master 'status' error : %s", mProtocol->StringFormatMessage(eventPtr).c_str());
      error = (int)mProtocol->ResponseCode(eventPtr);
    }
  }
  return error;
}



int MasterClient::SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler, uint32_t *msgIdp) {
  int error;

  if (msgIdp)
    *msgIdp = ((Header::HeaderT *)cbufPtr->data)->id;

  if ((error = mComm->SendRequest(mAddr, mTimeout, cbufPtr, handler)) != Error::OK) {
    LOG_VA_WARN("Comm::SendRequest to %s:%d failed - %s",
		inet_ntoa(mAddr.sin_addr), ntohs(mAddr.sin_port), Error::GetText(error));
  }
  return error;
}
