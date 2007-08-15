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

#include "Global.h"
#include "HdfsClient.h"
#include "HdfsProtocol.h"
#include "CommandCopyFromLocal.h"
#include "CommandCopyToLocal.h"
#include "CommandRemove.h"
#include "CommandLength.h"
#include "CommandMkdirs.h"
#include "CommandShutdown.h"

using namespace hypertable;
using namespace std;

#define DEFAULT_HOST "localhost"
#define DEFAULT_PORT 38546


namespace {
  const int COMMAND_COPY_TO_LOCAL    = 1;
  const int COMMAND_COPY_FROM_LOCAL  = 2;
  const int COMMAND_REMOVE           = 3;
  const int COMMAND_SHUTDOWN         = 4;
  const int COMMAND_LENGTH           = 5;
  const int COMMAND_MKDIRS           = 6;
  int Initialize(int argc, char **argv, struct sockaddr_in *addr, vector<const char *> &args);

  const char *usage[] = {
    "usage: hdfsClient [OPTIONS] <command> [<arg> ... ]",
    "",
    "OPTIONS:",
    "  --config=<fname>  Specifies the name of the config file.  [default",
    "                    is ../conf/placer.cfg relative to the executable]",
    "  --help            Display this usage text and exit",
    "  --verbose         Generate verbose logging output",
    "",
    "commands:",
    "  copyFromLocal <src> <dst>",
    "  copyToLocal [--seek=<n>] <src> <dst>",
    "  length <file>",
    "  remove <file>",
    "  mkdirs <dir>",
    "  shutdown [now]",
    "",
    "This program is used to perform hdfs operations.  It provides commands",
    "to copy files from the local filesystem to hdfs and vice-versa.  For",
    "detailed usage information for a command, run it with no arguments.",
    (const char *)0
  };

}



/**
 *  main
 */
int main(int argc, char **argv) {
  struct sockaddr_in addr;
  int error;
  int command;
  vector<const char *> args;

  if ((command = Initialize(argc, argv, &addr, args)) == 0)
    return 1;

  Global::comm = new Comm();
  Global::client = new HdfsClient(Global::comm, addr, 20);
  Global::protocol = new HdfsProtocol();

  if (!Global::client->WaitForConnection(30))
    goto done;

  if (command == COMMAND_COPY_TO_LOCAL)
    error = CommandCopyToLocal(args);
  else if (command == COMMAND_COPY_FROM_LOCAL)
    error = CommandCopyFromLocal(args);
  else if (command == COMMAND_LENGTH)
    cout << "LENGTH: " << CommandLength(args) << endl;
  else if (command == COMMAND_REMOVE)
    error = CommandRemove(args);
  else if (command == COMMAND_SHUTDOWN)
    error = CommandShutdown(args);
  else if (command == COMMAND_MKDIRS)
    error = CommandMkdirs(args);
  else
    Usage::DumpAndExit(usage);

 done:
  delete Global::client;
  delete Global::comm;
  delete Global::protocol;
  return 0;
}



namespace {

  /**
   *
   */
  int Initialize(int argc, char **argv, struct sockaddr_in *addr, vector<const char *> &args) {
    Properties *props = new Properties();
    string configFile;
    const char *command = 0;
    int commandVal;

    if (argc == 1 || (argc == 2 && !strcmp(argv[1], "--help")))
      Usage::DumpAndExit(usage);

    ReactorFactory::Initialize(1);
    System::Initialize(argv[0]);

    configFile = System::installDir + "/conf/placer.cfg";

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

    if (!strcmp(command, "copyToLocal"))
      commandVal = COMMAND_COPY_TO_LOCAL;
    else if (!strcmp(command, "copyFromLocal"))
      commandVal = COMMAND_COPY_FROM_LOCAL;
    else if (!strcmp(command, "remove"))
      commandVal = COMMAND_REMOVE;
    else if (!strcmp(command, "length"))
      commandVal = COMMAND_LENGTH;
    else if (!strcmp(command, "shutdown"))
      commandVal = COMMAND_SHUTDOWN;
    else if (!strcmp(command, "mkdirs"))
      commandVal = COMMAND_MKDIRS;
    else {
      LOG_VA_ERROR("Unrecognized command '%s'", command);
      return 0;
    }

    props->load(configFile);

    const char *host = props->getProperty("HdfsBroker.host", DEFAULT_HOST);

    int port = props->getPropertyInt("HdfsBroker.port", DEFAULT_PORT);

    cout << "HdfsBroker.host=" << host << endl;
    cout << "HdfsBroker.port=" << port << endl;

    memset(addr, 0, sizeof(struct sockaddr_in));
    {
      struct hostent *he = gethostbyname(host);
      if (he == 0) {
	herror("gethostbyname()");
	exit(1);
      }
      memcpy(&addr->sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
    }
    addr->sin_family = AF_INET;
    addr->sin_port = htons(port);

    return commandVal;
  }

}

