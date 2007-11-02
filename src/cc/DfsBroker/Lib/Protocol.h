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

#ifndef HYPERTABLE_DFSBROKER_PROTOCOL_H
#define HYPERTABLE_DFSBROKER_PROTOCOL_H

#include "AsyncComm/CommBuf.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/Protocol.h"

extern "C" {
#include <stdint.h>
#include <string.h>
}

namespace hypertable {

  namespace DfsBroker {

    class Protocol : public hypertable::Protocol {

    public:

      static CommBuf *CreateOpenRequest(std::string &fname, uint32_t bufferSize=0);

      static CommBuf *CreateCreateRequest(std::string &fname, bool overwrite, int32_t bufferSize,
				   int32_t replication, int64_t blockSize);

      static CommBuf *CreateCloseRequest(int32_t fd);

      static CommBuf *CreateReadRequest(int32_t fd, uint32_t amount);

      static CommBuf *CreateAppendRequest(int32_t fd, const void *buf, uint32_t amount);

      static CommBuf *CreateSeekRequest(int32_t fd, uint64_t offset);

      static CommBuf *CreateRemoveRequest(std::string &fname);

      static CommBuf *CreateLengthRequest(std::string &fname);

      static CommBuf *CreatePositionReadRequest(int32_t fd, uint64_t offset, uint32_t amount);

      static CommBuf *CreateMkdirsRequest(std::string &fname);

      static CommBuf *CreateRmdirRequest(std::string &fname);

      static CommBuf *CreateReaddirRequest(std::string &fname);

      static CommBuf *CreateFlushRequest(int32_t fd);

      static CommBuf *CreateStatusRequest();

      static CommBuf *CreateShutdownRequest(uint16_t flags);

      virtual const char *CommandText(short command);

      /**
       * Request Headers
       */

      typedef struct {
	uint16_t command;
      } __attribute__((packed)) RequestHeaderT;

      typedef struct {
	RequestHeaderT hdr;
	uint32_t       bufferSize;
      } __attribute__((packed)) RequestHeaderOpenT;

      typedef struct {
	RequestHeaderT hdr;
	int16_t        overwrite;
	int32_t        replication;
	int32_t        bufferSize;
	int64_t        blockSize;
      } __attribute__((packed)) RequestHeaderCreateT;

      typedef struct {
	RequestHeaderT hdr;
	int32_t        fd;
      } __attribute__((packed)) RequestHeaderCloseT;

      typedef RequestHeaderCloseT RequestHeaderFlushT;

      typedef struct {
	RequestHeaderT hdr;
	int32_t        fd;
	uint32_t       amount;
      } __attribute__((packed)) RequestHeaderReadT;

      typedef RequestHeaderReadT RequestHeaderAppendT;

      typedef struct {
	RequestHeaderT hdr;
	int32_t        fd;
	uint64_t       offset;
      } __attribute__((packed)) RequestHeaderSeekT;

      typedef  struct {
	RequestHeaderT hdr;
      } __attribute__((packed)) RequestHeaderRemoveT;

      typedef RequestHeaderRemoveT RequestHeaderLengthT;

      typedef RequestHeaderRemoveT RequestHeaderMkdirsT;

      typedef RequestHeaderRemoveT RequestHeaderRmdirT;

      typedef RequestHeaderRemoveT RequestHeaderReaddirT;

      typedef struct {
	RequestHeaderT hdr;
	int32_t        fd;
	uint64_t       offset;
	uint32_t       amount;
      } __attribute__((packed)) RequestHeaderPositionReadT;

      /**
       * Response Headers
       */

      typedef struct {
	int32_t  error;
	int32_t  handle;
      } __attribute__((packed)) ResponseHeaderOpenT;

      typedef ResponseHeaderOpenT ResponseHeaderCreateT;

      typedef struct {
	int32_t  error;
	uint64_t offset;
	int32_t  amount;
      } __attribute__((packed)) ResponseHeaderReadT;

      typedef ResponseHeaderReadT ResponseHeaderAppendT;

      typedef ResponseHeaderOpenT ResponseHeaderRemoveT;

      typedef ResponseHeaderOpenT ResponseHeaderMkdirsT;

      typedef struct {
	int32_t  error;
	int64_t  length;
      } __attribute__((packed)) ResponseHeaderLengthT;

      static const uint16_t COMMAND_OPEN     = 0;
      static const uint16_t COMMAND_CREATE   = 1;
      static const uint16_t COMMAND_CLOSE    = 2;
      static const uint16_t COMMAND_READ     = 3;
      static const uint16_t COMMAND_APPEND   = 4;
      static const uint16_t COMMAND_SEEK     = 5;
      static const uint16_t COMMAND_REMOVE   = 6;
      static const uint16_t COMMAND_SHUTDOWN = 7;
      static const uint16_t COMMAND_LENGTH   = 8;
      static const uint16_t COMMAND_PREAD    = 9;
      static const uint16_t COMMAND_MKDIRS   = 10;
      static const uint16_t COMMAND_STATUS   = 11;
      static const uint16_t COMMAND_FLUSH    = 12;
      static const uint16_t COMMAND_RMDIR    = 13;
      static const uint16_t COMMAND_READDIR  = 14;
      static const uint16_t COMMAND_MAX      = 15;

      static const uint16_t SHUTDOWN_FLAG_IMMEDIATE = 0x0001;

      static const char * msCommandStrings[COMMAND_MAX];

    };

  }

}

#endif // HYPERTABLE_DFSBROKER_PROTOCOL_H
