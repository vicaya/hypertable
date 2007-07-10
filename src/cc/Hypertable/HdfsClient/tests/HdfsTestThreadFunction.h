/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


extern "C" {
#include <netinet/in.h>
}

/**
 *  Forward declarations
 */
namespace hypertable {
  class Client;
}

class HdfsTestThreadFunction {
 public:
  HdfsTestThreadFunction(struct sockaddr_in &addr, const char *input) : mAddr(addr) {
    mInputFile = input;
  }
  void SetHdfsFile(const char *hdfsFile) { 
    mHdfsFile = hdfsFile;
  }
  void SetOutputFile(const char *output) { 
    mOutputFile = output;
  }
  void operator()();

 private:
  struct sockaddr_in mAddr;
  const char *mInputFile;
  const char *mOutputFile;
  const char *mHdfsFile;
};

