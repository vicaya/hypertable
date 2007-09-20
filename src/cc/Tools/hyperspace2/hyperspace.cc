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

/**
#include "Hyperspace/HyperspaceClient.h"
#include "Hyperspace/HyperspaceProtocol.h"
#include "CommandCreate.h"
#include "CommandDelete.h"
#include "CommandMkdirs.h"
#include "CommandAttrSet.h"
#include "CommandAttrGet.h"
#include "CommandAttrDel.h"
#include "CommandExists.h"
**/

#include "Hyperspace/Session.h"

using namespace hypertable;
using namespace std;


namespace {

  const char *usage[] = {
    "",
    "usage: hyperspace [OPTIONS] <command> [<arg> ... ]",
    "",
    "OPTIONS:",
    "  --config=<fname>  Specifies the name of the config file.  [default",
    "                    is ../conf/hypertable.cfg relative to the executable]",
    "",
    "commands:",
    "  mkdirs <dir>",
    "  touch <file>",
    "  delete <file>",
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
 * 
 */
class SessionHandler : public SessionCallback {
public:
  virtual void Jeopardy() { cerr << "Jeopardy" << endl; }
  virtual void Safe() { cerr << "Safe" << endl; }
  virtual void Expired() { cerr << "Expired" << endl; }
};



/**
 *  main
 */
int main(int argc, char **argv) {
  PropertiesPtr propsPtr( new Properties() );
  Comm *comm;
  int error = 0;
  string configFile;
  vector<const char *> args;
  Hyperspace::Session *session;
  SessionHandler sessionHandler;
  bool verbose = false;

  if (argc == 1)
    Usage::DumpAndExit(usage);

  System::Initialize(argv[0]);
  ReactorFactory::Initialize(1);

  configFile = System::installDir + "/conf/hypertable.cfg";

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      configFile = &argv[i][9];
    else if (!strcmp(argv[i], "--verbose"))
      verbose = true;
  }

  propsPtr->load(configFile);

  if (verbose)
    propsPtr->setProperty("verbose", "true");

  comm = new Comm();

  session = new Session(comm, propsPtr, &sessionHandler);

  if (!session->WaitForConnection(30))
    goto done;

  poll(0, 0, -1);

#if 0
  if (!strcmp(command, "mkdirs"))
    error = CommandMkdirs(hyperspaceClient, args);
  else if (!strcmp(command, "touch"))
    error = CommandCreate(hyperspaceClient, args);
  else if (!strcmp(command, "delete"))
    error = CommandDelete(hyperspaceClient, args);
  else if (!strcmp(command, "attrset"))
    error = CommandAttrSet(hyperspaceClient, args);
  else if (!strcmp(command, "attrget"))
    error = CommandAttrGet(hyperspaceClient, args);
  else if (!strcmp(command, "attrdel"))
    error = CommandAttrDel(hyperspaceClient, args);
  else if (!strcmp(command, "exists"))
    return CommandExists(hyperspaceClient, args);
  else {
    LOG_VA_ERROR("Unrecognized command '%s'", command);
    exit(1);
  }

  if (error != Error::OK)
    cerr << "Problem executing command - " << Error::GetText(error) << endl;

#endif

 done:
  delete session;
  delete comm;
  return error;
}

