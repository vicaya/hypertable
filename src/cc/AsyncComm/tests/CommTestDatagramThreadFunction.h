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

extern "C" {
#include <netinet/in.h>
}

/**
 *  Forward declarations
 */
namespace hypertable {
  class Comm;
}

class CommTestDatagramThreadFunction {
 public:
  CommTestDatagramThreadFunction(hypertable::Comm *comm, struct sockaddr_in &addr, const char *input) : mComm(comm), mAddr(addr) {
    mInputFile = input;
    mPort = 0;
  }
  void SetOutputFile(const char *output) { 
    mOutputFile = output;
  }
  void SetReceivePort(uint16_t port) { mPort = port; }
  void operator()();

 private:
  hypertable::Comm *mComm;
  struct sockaddr_in mAddr;
  const char *mInputFile;
  const char *mOutputFile;
  uint16_t mPort;
};

