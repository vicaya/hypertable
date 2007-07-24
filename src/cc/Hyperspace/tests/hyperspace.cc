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


#include <string>
#include <vector>

extern "C" {
#include <sys/poll.h>
}

#include <boost/thread/exceptions.hpp>

#include "AsyncComm/Comm.h"
#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "../HyperspaceClient.h"
#include "../HyperspaceProtocol.h"
#include "CommandCreate.h"
#include "CommandMkdirs.h"
#include "CommandAttrSet.h"
#include "CommandAttrGet.h"
#include "CommandAttrDel.h"
#include "CommandExists.h"

using namespace hypertable;
using namespace std;


namespace {

  const char *usage[] = {
    "",
    "usage: hyperspace [OPTIONS] <command> [<arg> ... ]",
    "",
    "OPTIONS:",
    "  --config=<fname>  Specifies the name of the config file.  [default",
    "                    is ../conf/placer.cfg relative to the executable]",
    "",
    "commands:",
    "  mkdirs <dir>",
    "  touch <file>",
    "  attrset <file> <attr> <value>",
    "  attrget <file> <attr>",
    "  attrdel <file> <attr>",
    "  exists <file>",
    "",
    "This program is used to test Placer FS operations.  To help for an",
    "individual command, run this program with the command name and no",
    "arguments."
    "",
    (const char *)0
  };

}



/**
 *  main
 */
int main(int argc, char **argv) {
  Properties *props = new Properties();
  Comm *comm;
  HyperspaceClient *hyperspaceClient;
  int error;
  const char *command = 0;
  string configFile;
  vector<const char *> args;

  if (argc == 1)
    Usage::DumpAndExit(usage);

  System::Initialize(argv[0]);
  ReactorFactory::Initialize(1);

  configFile = System::installDir + "/conf/hypertable.cfg";

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      configFile = &argv[i][9];
    else if (command == 0)
      command = argv[i];
    else
      args.push_back(argv[i]);
  }

  if (command == 0)
    Usage::DumpAndExit(usage);

  props->load(configFile);

  comm = new Comm(0);

  hyperspaceClient = new HyperspaceClient(comm, props);

  if (!hyperspaceClient->WaitForConnection())
    goto done;

  if (!strcmp(command, "mkdirs"))
    error = CommandMkdirs(hyperspaceClient, args);
  else if (!strcmp(command, "touch"))
    error = CommandCreate(hyperspaceClient, args);
  else if (!strcmp(command, "attrset"))
    error = CommandAttrSet(hyperspaceClient, args);
  else if (!strcmp(command, "attrget"))
    error = CommandAttrGet(hyperspaceClient, args);
  else if (!strcmp(command, "attrdel"))
    error = CommandAttrDel(hyperspaceClient, args);
  else if (!strcmp(command, "exists"))
    error = CommandExists(hyperspaceClient, args);
  else {
    LOG_VA_ERROR("Unrecognized command '%s'", command);
    exit(1);
  }

 done:
  delete hyperspaceClient;
  delete comm;
  delete props;
  return 0;
}

