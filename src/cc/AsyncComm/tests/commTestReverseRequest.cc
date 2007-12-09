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

#include <cstdlib>

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
#include "Common/ServerLauncher.h"
#include "Common/StringExt.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/Event.h"
#include "AsyncComm/ReactorFactory.h"

#include "CommTestThreadFunction.h"

using namespace Hypertable;

namespace {
  const char *usage[] = {
    "usage: commTestReverseRequest",
    "",
    "This program ...",
    0
  };

}


int main(int argc, char **argv) {
  std::vector<const char *> clientArgs;
  std::vector<const char *> serverArgs;

  if (argc != 1)
    Usage::dump_and_exit(usage);

  srand(8876);

  System::initialize(argv[0]);
  ReactorFactory::initialize(1);

  clientArgs.push_back("sampleClient");
  clientArgs.push_back("--recv-addr=localhost:12789");
  clientArgs.push_back("datafile.txt");
  clientArgs.push_back((const char *)0);

  serverArgs.push_back("testServer");
  serverArgs.push_back("--connect-to=localhost:12789");
  serverArgs.push_back((const char *)0);

  {
    ServerLauncher client("./sampleClient", (char * const *)&clientArgs[0], "commTestReverseRequest.out");
    poll(0, 0, 2000);
    ServerLauncher server("./testServer", (char * const *)&serverArgs[0]);
  }

  if (system("diff commTestReverseRequest.out commTestReverseRequest.golden"))
    return 1;

  return 0;
}
