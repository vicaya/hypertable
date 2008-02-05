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
#include <cstring>
#include <iostream>
#include <vector>

extern "C" {
#include <readline/readline.h>
#include <readline/history.h>
}

#include "Common/InteractiveCommand.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"

#include "Hypertable/Lib/RangeServerClient.h"
#include "Hyperspace/Session.h"

#include "CommandCreateScanner.h"
#include "CommandDestroyScanner.h"
#include "CommandFetchScanblock.h"
#include "CommandLoadRange.h"
#include "CommandUpdate.h"
#include "CommandShutdown.h"

using namespace Hypertable;
using namespace std;

namespace {

  char *line_read = 0;

  char *rl_gets () {

    if (line_read) {
      free (line_read);
      line_read = (char *)NULL;
    }

    /* Get a line from the user. */
    line_read = readline ("rsclient> ");

    /* If the line has any text in it, save it on the history. */
    if (line_read && *line_read)
      add_history (line_read);

    return line_read;
  }

  /**
   * Parses the --server= command line argument.
   */
  void ParseServerArgument(const char *arg, std::string &host, uint16_t *portPtr) {
    const char *ptr = strchr(arg, ':');
    *portPtr = 0;
    if (ptr) {
      host = string(arg, ptr-arg);
      *portPtr = (uint16_t)atoi(ptr+1);
    }
    else
      host = arg;
    return;
  }


  /**
   *
   */
  void BuildInetAddress(struct sockaddr_in &addr, PropertiesPtr &props_ptr, std::string &userSuppliedHost, uint16_t userSuppliedPort) {
    std::string host;
    uint16_t port;

    if (userSuppliedHost == "")
      host = "localhost";
    else
      host = userSuppliedHost;

    if (userSuppliedPort == 0)
      port = (uint16_t)props_ptr->get_int("Hypertable.RangeServer.Port", 38060);
    else
      port = userSuppliedPort;

    memset(&addr, 0, sizeof(struct sockaddr_in));
    {
      struct hostent *he = gethostbyname(host.c_str());
      if (he == 0) {
	herror("gethostbyname()");
	exit(1);
      }
      memcpy(&addr.sin_addr.s_addr, he->h_addr_list[0], sizeof(uint32_t));
    }
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
  }


  const char *usage[] = {
    "usage: hypertable [OPTIONS]",
    "",
    "OPTIONS:",
    "",
    "  --config=<file>   Read configuration from <file>.  The default config",
    "     file is \"conf/hypertable.cfg\" relative to the toplevel install",
    "     directory",
    "",
    "  --server=<host>[:<port>]  Connect to Range server at machine <host>",
    "     and port <port>.  By default it connects to localhost:38060.",
    "",
    "  --help  Display this help text and exit.",
    "",
    "This program provides a command line interface to a Range Server.",
    (const char *)0
  };

  const char *helpHeader[] = {
    "",
    "The following commands can be used to send requests to a Range Server.",
    "In the command descriptions below, the argument <range> has the format",
    "<table>[<startRow>:<endRow>].  For example, to specify a range from",
    "table Test that has a start row of 'carrot' and an end row of 'kerchoo',",
    "you would type the following:",
    "",
    "  Test[carrot:kerchoo]",
    "",
    "To specify a range that starts at the beginning of a table or one that",
    "spans to the end of a table, you can omit the row key as follows:",
    "",
    "  Test[carrot:]",
    "  Test[:kerchoo]",
    "",
    "",
    "Command Summary",
    "---------------",
    "",
    (const char *)0
  };

  const char *helpTrailer[] = {
    "help",
    "",
    "  Display this help text.",
    "",
    "quit",
    "",
    "  Exit the program.",
    "",
    (const char *)0
  };
  
}


/**
 *
 */
int main(int argc, char **argv) {
  const char *line;
  string configFile = "";
  string host = "";
  uint16_t port = 0;
  struct sockaddr_in addr;
  vector<InteractiveCommand *>  commands;
  Comm *comm;
  ConnectionManager *connManager;
  PropertiesPtr props_ptr;
  CommandFetchScanblock *fetchScanblock;
  Hyperspace::SessionPtr hyperspace_ptr;
  RangeServerClientPtr range_server_ptr;
  std::string history_file = (std::string)getenv("HOME") + "/.rsclient_history";

  System::initialize(argv[0]);
  ReactorFactory::initialize((uint16_t)System::get_processor_count());

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      configFile = &argv[i][9];
    else if (!strncmp(argv[i], "--server=", 9))
      ParseServerArgument(&argv[i][9], host, &port);
    else
      Usage::dump_and_exit(usage);
  }

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  props_ptr = new Properties(configFile);

  BuildInetAddress(addr, props_ptr, host, port);

  read_history(history_file.c_str());

  comm = new Comm();
  connManager = new ConnectionManager(comm);

  // Create Range Server client object
  range_server_ptr = new RangeServerClient(comm, 30);

  // Connect to Range Server
  connManager->add(addr, 30, "Range Server");
  if (!connManager->wait_for_connection(addr, 30))
    cerr << "Timed out waiting for for connection to Range Server.  Exiting ..." << endl;

  // Connect to Hyperspace
  hyperspace_ptr = new Hyperspace::Session(connManager->get_comm(), props_ptr, 0);
  if (!hyperspace_ptr->wait_for_connection(30))
    exit(1);

  commands.push_back( new CommandCreateScanner(addr, range_server_ptr, hyperspace_ptr) );  
  commands.push_back( new CommandDestroyScanner(addr, range_server_ptr) );
  fetchScanblock = new CommandFetchScanblock(addr, range_server_ptr);
  commands.push_back( fetchScanblock );
  commands.push_back( new CommandLoadRange(addr, range_server_ptr, hyperspace_ptr) );
  commands.push_back( new CommandUpdate(addr, range_server_ptr, hyperspace_ptr) );
  commands.push_back( new CommandShutdown(addr, range_server_ptr) );

  cout << "Welcome to the Range Server command interpreter." << endl;
  cout << "Type 'help' for a description of commands." << endl;
  cout << endl << flush;

  using_history();
  while ((line = rl_gets()) != 0) {
    size_t i;

    if (*line == 0) {
      if (CommandCreateScanner::ms_scanner_id != -1) {
	fetchScanblock->clear_args();
	fetchScanblock->run();
      }
      continue;
    }

    for (i=0; i<commands.size(); i++) {
      if (commands[i]->matches(line)) {
	commands[i]->parse_command_line(line);
	commands[i]->run();
	break;
      }
    }

    if (i == commands.size()) {
      if (!strcmp(line, "quit") || !strcmp(line, "exit")) {
	write_history(history_file.c_str());
	exit(0);
      }
      else if (!strcmp(line, "help")) {
	Usage::dump(helpHeader);
	for (i=0; i<commands.size(); i++) {
	  Usage::dump(commands[i]->usage());
	  cout << endl;
	}
	Usage::dump(helpTrailer);
      }
      else
	cout << "Unrecognized command." << endl;
    }

  }

  write_history(history_file.c_str());
  return 0;
}
