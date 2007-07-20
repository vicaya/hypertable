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
#include <poll.h>
#include <signal.h>
#include <sys/types.h>
#include <unistd.h>
}

#include <boost/thread/thread.hpp>

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/TestHarness.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ReactorFactory.h"

#include "HdfsTestThreadFunction.h"

#include "../Global.h"

using namespace hypertable;
using namespace hypertable;

namespace {
  const short DEFAULT_PORT = 38546;
  const char *usage[] = {
    "usage: hdfsTest",
    "",
    "This program ...",
    0
  };
}


int main(int argc, char **argv) {
  boost::thread  *thread1, *thread2;
  struct sockaddr_in addr;

  if (argc != 1)
    Usage::DumpAndExit(usage);

  System::Initialize(argv[0]);
  ReactorFactory::Initialize(1);

  memset(&addr, 0, sizeof(struct sockaddr_in));
  {
    struct hostent *he = gethostbyname("localhost");
    if (he == 0) {
      herror("gethostbyname()");
      return 1;
    }
    memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
  }
  addr.sin_family = AF_INET;
  addr.sin_port = htons(DEFAULT_PORT);

  Global::comm = new Comm(0);
  Global::client = new HdfsClient(Global::comm, addr, 15);
  Global::protocol = new HdfsProtocol();

  if (!Global::client->WaitForConnection(15)) {
    LOG_ERROR("Unable to connect to HDFS");
    return 1;
  }

  HdfsTestThreadFunction threadFunc(addr, "/usr/share/dict/words");

  threadFunc.SetHdfsFile("/RegressionTests/hdfs/output.a");
  threadFunc.SetOutputFile("output.a");
  thread1 = new boost::thread(threadFunc);

  threadFunc.SetHdfsFile("/RegressionTests/hdfs/output.b");
  threadFunc.SetOutputFile("output.b");
  thread2 = new boost::thread(threadFunc);

  thread1->join();
  thread2->join();

  if (system("diff /usr/share/dict/words output.a"))
    return 1;

  if (system("diff output.a output.b"))
    return 1;

  return 0;
}
