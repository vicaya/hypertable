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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
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
  std::vector<const char *> client_args;
  std::vector<const char *> server_args;

  if (argc != 1)
    Usage::dump_and_exit(usage);

  srand(8876);

  System::initialize(System::locate_install_dir(argv[0]));
  ReactorFactory::initialize(1);

  client_args.push_back("sampleClient");
  client_args.push_back("--recv-addr=localhost:12789");
  client_args.push_back("datafile.txt");
  client_args.push_back((const char *)0);

  server_args.push_back("testServer");
  server_args.push_back("--connect-to=localhost:12789");
  server_args.push_back((const char *)0);

  {
    ServerLauncher client("./sampleClient", (char * const *)&client_args[0], "commTestReverseRequest.out");
    poll(0, 0, 2000);
    ServerLauncher server("./testServer", (char * const *)&server_args[0]);
  }

  if (system("diff commTestReverseRequest.out commTestReverseRequest.golden"))
    return 1;

  return 0;
}
