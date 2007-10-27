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
#include "Common/InetAddr.h"

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/DispatchHandlerSynchronizer.h"
#include "AsyncComm/Serialization.h"

#include "Hyperspace/Session.h"

#include "MasterFileHandler.h"
#include "MasterClient.h"
#include "MasterProtocol.h"


/**
 * 
 */
MasterClient::MasterClient(ConnectionManager *connManager, Hyperspace::Session *hyperspace, time_t timeout, ApplicationQueue *appQueue) : mVerbose(true), mConnManager(connManager), mHyperspace(hyperspace), mTimeout(timeout), mAppQueue(appQueue), mInitiated(false) {
  int error;

  mComm = mConnManager->GetComm();
  memset(&mMasterAddr, 0, sizeof(mMasterAddr));

  /**
   * Open /hypertable/master Hyperspace file to discover the master.
   */
  mMasterFileCallbackPtr = new MasterFileHandler(this, mAppQueue);
  if ((error = mHyperspace->Open("/hypertable/master", OPEN_FLAG_READ, mMasterFileCallbackPtr, &mMasterFileHandle)) != Error::OK) {
    LOG_VA_ERROR("Unable to open Hyperspace file '/hypertable/master' (%s)  Try re-running with --initialize", Error::GetText(error));
    exit(1);
  }

}



MasterClient::~MasterClient() {
  mHyperspace->Close(mMasterFileHandle);
}



/**
 *
 */
int MasterClient::InitiateConnection(DispatchHandlerPtr dispatchHandlerPtr) {
  mDispatcherHandlerPtr = dispatchHandlerPtr;
  return ReloadMaster();
}



int MasterClient::CreateTable(const char *tableName, const char *schemaString, DispatchHandler *handler) {
  CommBufPtr cbufPtr( MasterProtocol::CreateCreateTableRequest(tableName, schemaString) );
  return SendMessage(cbufPtr, handler);
}



int MasterClient::CreateTable(const char *tableName, const char *schemaString) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( MasterProtocol::CreateCreateTableRequest(tableName, schemaString) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      if (mVerbose)
	LOG_VA_ERROR("Master 'create table' error, tableName=%s : %s", tableName, MasterProtocol::StringFormatMessage(eventPtr).c_str());
      error = (int)MasterProtocol::ResponseCode(eventPtr);
    }
  }
  return error;
}


int MasterClient::GetSchema(const char *tableName, DispatchHandler *handler) {
  CommBufPtr cbufPtr( MasterProtocol::CreateGetSchemaRequest(tableName) );
  return SendMessage(cbufPtr, handler);
}


int MasterClient::GetSchema(const char *tableName, std::string &schema) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( MasterProtocol::CreateGetSchemaRequest(tableName) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      if (mVerbose)
	LOG_VA_ERROR("Master 'get schema' error, tableName=%s : %s", tableName, MasterProtocol::StringFormatMessage(eventPtr).c_str());
      error = (int)MasterProtocol::ResponseCode(eventPtr);
    }
    else {
      uint8_t *ptr = eventPtr->message + sizeof(int32_t);
      size_t remaining = eventPtr->messageLen - sizeof(int32_t);
      const char *schemaStr;
      if (!Serialization::DecodeString(&ptr, &remaining, &schemaStr)) {
	if (mVerbose)
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
  CommBufPtr cbufPtr( MasterProtocol::CreateStatusRequest() );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      if (mVerbose)
	LOG_VA_ERROR("Master 'status' error : %s", MasterProtocol::StringFormatMessage(eventPtr).c_str());
      error = (int)MasterProtocol::ResponseCode(eventPtr);
    }
  }
  return error;
}


int MasterClient::RegisterServer(std::string &serverIdStr, DispatchHandler *handler) {
  CommBufPtr cbufPtr( MasterProtocol::CreateRegisterServerRequest(serverIdStr) );
  return SendMessage(cbufPtr, handler);
}


int MasterClient::RegisterServer(std::string &serverIdStr) {
  DispatchHandlerSynchronizer syncHandler;
  EventPtr eventPtr;
  CommBufPtr cbufPtr( MasterProtocol::CreateRegisterServerRequest(serverIdStr) );
  int error = SendMessage(cbufPtr, &syncHandler);
  if (error == Error::OK) {
    if (!syncHandler.WaitForReply(eventPtr)) {
      if (mVerbose)
	LOG_VA_ERROR("Master 'register server' error : %s", MasterProtocol::StringFormatMessage(eventPtr).c_str());
      error = (int)MasterProtocol::ResponseCode(eventPtr);
    }
  }
  return error;
}



int MasterClient::SendMessage(CommBufPtr &cbufPtr, DispatchHandler *handler) {
  boost::mutex::scoped_lock lock(mMutex);
  int error;

  if ((error = mComm->SendRequest(mMasterAddr, mTimeout, cbufPtr, handler)) != Error::OK) {
    std::string addrStr;
    if (mVerbose)
      LOG_VA_WARN("Comm::SendRequest to %s failed - %s", InetAddr::StringFormat(addrStr, mMasterAddr), Error::GetText(error));
  }

  return error;
}


/**
 * 
 */
int MasterClient::ReloadMaster() {
  boost::mutex::scoped_lock lock(mMutex);
  int error;
  DynamicBuffer value(0);
  std::string addrStr;

  if ((error = mHyperspace->AttrGet(mMasterFileHandle, "address", value)) != Error::OK) {
    if (mVerbose)
      LOG_VA_ERROR("Problem reading 'address' attribute of Hyperspace file /hypertable/master - %s", Error::GetText(error));
    return Error::MASTER_NOT_RUNNING;
  }

  addrStr = (const char *)value.buf;

  if (addrStr != mMasterAddrString) {

    if (mMasterAddr.sin_port != 0) {
      if ((error = mConnManager->Remove(mMasterAddr)) != Error::OK) {
	if (mVerbose)
	  LOG_VA_WARN("Problem removing connection to Master - %s", Error::GetText(error));
      }
      if (mVerbose)
	LOG_VA_INFO("Connecting to new Master (old=%s, new=%s)", mMasterAddrString.c_str(), addrStr.c_str());
    }

    mMasterAddrString = addrStr;

    InetAddr::Initialize(&mMasterAddr, mMasterAddrString.c_str());

    mConnManager->Add(mMasterAddr, 15, "Master", mDispatcherHandlerPtr.get());
  }

  return Error::OK;
}


bool MasterClient::WaitForConnection(long maxWaitSecs) {
  return mConnManager->WaitForConnection(mMasterAddr, maxWaitSecs);
}
