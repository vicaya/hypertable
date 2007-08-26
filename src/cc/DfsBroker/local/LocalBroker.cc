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

#include "LocalBroker.h"

using namespace hypertable;

LocalBroker::LocalBroker(PropertiesPtr &propsPtr) : mVerbose(false) {
  mVerbose = propsPtr->getPropertyBool("verbose", false);
}

LocalBroker::~LocalBroker() {
}


void LocalBroker::Open(ResponseCallbackOpen *cb, const char *fieName, uint32_t bufferSize) {
  return;
}

void LocalBroker::Create(ResponseCallbackOpen *cb, const char *fileName, bool overwrite,
	    uint32_t bufferSize, uint16_t replication, uint64_t blockSize) {
  return;
}

void LocalBroker::Close(ResponseCallback *cb, uint32_t fd) {
  return;
}

void LocalBroker::Read(ResponseCallbackRead *cb, uint32_t fd, uint32_t amount) {
  return;
}

void LocalBroker::Append(ResponseCallbackAppend *cb, uint32_t fd, uint32_t amount, uint8_t *data) {
  return;
}

void LocalBroker::Seek(ResponseCallback *cb, uint32_t fd, uint64_t offset) {
  return;
}

void LocalBroker::Remove(ResponseCallback *cb, const char *fileName) {
  return;
}

void LocalBroker::Length(ResponseCallbackLength *cb, const char *fieName) {
  return;
}

void LocalBroker::Pread(ResponseCallbackRead *cb, uint32_t fd, uint64_t offset, uint32_t amount) {
  return;
}

void LocalBroker::Mkdirs(ResponseCallback *cb, const char *dirName) {
  return;
}

void LocalBroker::Flush(ResponseCallback *cb, uint32_t fd) {
  return;
}

void LocalBroker::Status(ResponseCallback *cb) {
  return;
}

void LocalBroker::Shutdown(ResponseCallback *cb, uint16_t flags) {
  return;
}
