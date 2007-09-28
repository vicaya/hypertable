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

#include <cstdio>
#include <string>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/thread/exceptions.hpp>

extern "C" {
#include <readline/history.h>
#include <readline/readline.h>
#include <sys/poll.h>
}

#include "AsyncComm/Comm.h"
#include "Common/Error.h"
#include "Common/InteractiveCommand.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "Global.h"
#include "Hyperspace/Session.h"

#include "CommandMkdir.h"
#include "CommandDelete.h"
#include "CommandOpen.h"
#include "CommandClose.h"
#include "CommandAttrSet.h"
#include "CommandAttrGet.h"
#include "CommandAttrDel.h"
#include "CommandExists.h"
#include "CommandLock.h"
#include "CommandTryLock.h"
#include "CommandRelease.h"

using namespace hypertable;
using namespace std;


namespace {

  char *line_read = 0;

  bool gTestMode = false;
  std::string gInputStr;

  char *rl_gets () {

    if (gTestMode) {
      getline(cin, gInputStr);
      boost::trim(gInputStr);
      cout << gInputStr << endl;
      return (char *)gInputStr.c_str();
    }
    else {
      if (line_read) {
	free (line_read);
	line_read = (char *)NULL;
      }

      /* Get a line from the user. */
      line_read = readline("hyperspace> ");

      /* If the line has any text in it, save it on the history. */
      if (line_read && *line_read)
	add_history (line_read);

      return line_read;
    }
  }

  const char *usage[] = {
    "usage: hyperspace [OPTIONS]",
    "",
    "OPTIONS:",
    "  --config=<file>  Read configuration from <file>.  The default config file is",
    "                   \"conf/hypertable.cfg\" relative to the toplevel install",
    "                   directory",
    "  --debug          Turn on debugging output",
    "  --eval <cmds>    Evaluates the commands in the string <cmds>.  Several",
    "                   commands can be supplied in <cmds> by separating them with",
    "                   semicolons",
    "  --no-prompt      Don't display a command prompt",
    "  --help           Display this help text and exit",
    "  --test-mode      Suppress file line number information from error messages to",
    "                   simplify diff'ing test output.",
    "",
    "This is a command interpreter for Hyperspace, a global namespace and lock service",
    "for loosely-coupled distributed systems.",
    (const char *)0
  };

  const char *helpTrailer[] = {
    "help",
    "  Display this help text.",
    "",
    "quit",
    "  Exit the program.",
    "",
    (const char *)0
  };
}


/**
 * 
 */
class SessionHandler : public SessionCallback {
public:
  virtual void Jeopardy() { cout << "SESSION CALLBACK: Jeopardy" << endl << flush; }
  virtual void Safe() { cout << "SESSION CALLBACK: Safe" << endl << flush; }
  virtual void Expired() { cout << "SESSION CALLBACK: Expired" << endl << flush; }
};



/**
 *  main
 */
int main(int argc, char **argv, char **envp) {
  const char *line;
  char *eval = 0;
  size_t i;
  string configFile = "";
  vector<InteractiveCommand *>  commands;
  Comm *comm;
  ConnectionManager *connManager;
  PropertiesPtr propsPtr;
  Hyperspace::Session *session;
  SessionHandler sessionHandler;
  bool verbose = false;
  int error;

  System::Initialize(argv[0]);
  ReactorFactory::Initialize((uint16_t)System::GetProcessorCount());

  configFile = System::installDir + "/conf/hypertable.cfg";

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      configFile = &argv[i][9];
    else if (!strcmp(argv[i], "--debug"))
      verbose = true;
    else if (!strcmp(argv[i], "--eval")) {
      i++;
      if (i == argc)
	Usage::DumpAndExit(usage);
      eval = argv[i];
    }
    else if (!strcmp(argv[i], "--test-mode")) {
      Logger::SetTestMode("hyperspace");
      gTestMode = true;
    }
    else
      Usage::DumpAndExit(usage);
  }

  propsPtr.reset( new Properties( configFile ) );

  if (verbose)
    propsPtr->setProperty("verbose", "true");

  comm = new Comm();

  session = new Session(comm, propsPtr, &sessionHandler);

  if (!session->WaitForConnection(30)) {
    cerr << "Unable to establish session with Hyerspace, exiting..." << endl;
    exit(1);
  }

  commands.push_back( new CommandMkdir(session) );
  commands.push_back( new CommandDelete(session) );
  commands.push_back( new CommandOpen(session) );
  commands.push_back( new CommandClose(session) );
  commands.push_back( new CommandAttrSet(session) );
  commands.push_back( new CommandAttrGet(session) );
  commands.push_back( new CommandAttrDel(session) );
  commands.push_back( new CommandExists(session) );
  commands.push_back( new CommandLock(session) );
  commands.push_back( new CommandTryLock(session) );
  commands.push_back( new CommandRelease(session) );

  /**
   * Non-interactive mode
   */
  if (eval != 0) {
    const char *str;
    std::string commandStr;
    str = strtok(eval, ";");
    while (str) {
      commandStr = str;
      boost::trim(commandStr);
      for (i=0; i<commands.size(); i++) {
	if (commands[i]->Matches(commandStr.c_str())) {
	  commands[i]->ParseCommandLine(commandStr.c_str());
	  if ((error = commands[i]->run()) != Error::OK) {
	    cerr << Error::GetText(error) << endl;
	    return 1;
	  }
	  break;
	}
      }
      if (i == commands.size()) {
	LOG_VA_ERROR("Unrecognized command : %s", commandStr.c_str());
	return 1;
      }
      str = strtok(0, ";");      
    }
    return 0;
  }

  cout << "Welcome to the Hyperspace command interpreter.  Hyperspace" << endl;
  cout << "is a global namespace and lock service for loosely-coupled" << endl;
  cout << "distributed systems.  Type 'help' for a description of commands." << endl;
  cout << endl << flush;

  using_history();
  while ((line = rl_gets()) != 0) {

    if (*line == 0)
      continue;

    for (i=0; i<commands.size(); i++) {
      if (commands[i]->Matches(line)) {
	commands[i]->ParseCommandLine(line);
	if ((error = commands[i]->run()) != Error::OK && error != -1)
	  cout << Error::GetText(error) << endl;
	break;
      }
    }

    if (i == commands.size()) {
      if (!strcmp(line, "quit") || !strcmp(line, "exit"))
	exit(0);
      else if (!strcmp(line, "pwd"))
	cout << Global::cwd << endl;
      else if (!strcmp(line, "help")) {
	cout << endl;
	for (i=0; i<commands.size(); i++) {
	  Usage::Dump(commands[i]->Usage());
	  cout << endl;
	}
	Usage::Dump(helpTrailer);
      }
      else
	cout << "Unrecognized command." << endl;
    }

  }

 done:
  delete session;
  delete comm;
  return error;
}

