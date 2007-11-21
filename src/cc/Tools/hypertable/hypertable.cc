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
#include <queue>

extern "C" {
#include <readline/readline.h>
#include <readline/history.h>
}

#include <boost/algorithm/string.hpp>
#include <boost/thread/exceptions.hpp>

#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/HqlCommandInterpreter.h"

using namespace Hypertable;
using namespace std;

namespace {

  string g_accum;
  bool g_batch_mode = false;
  bool g_cont = false;
  char *line_read = 0;

  char *rl_gets () {

    if (line_read) {
      free (line_read);
      line_read = (char *)NULL;
    }

    /* Get a line from the user. */
    if (g_batch_mode)
      line_read = readline(0);
    else if (!g_cont)
      line_read = readline("hypertable> ");
    else
      line_read = readline("         -> ");

    /* If the line has any text in it, save it on the history. */
    if (!g_batch_mode && line_read && *line_read)
      add_history (line_read);

    return line_read;
  }

  const char *usage[] = {
    "usage: hql [OPTIONS]",
    "",
    "OPTIONS:",
    "",
    "  -B, --batch    Disable interactive behavior.",
    "  --config=<f>   Read configuration from <f>.  The default config",
    "     file is \"conf/hypertable.cfg\" relative to the toplevel install",
    "     directory",
    "",
    "  --help  Display this help text and exit.",
    "",
    "This program is the command line interpreter for HQL.",
    (const char *)0
  };

}


int main(int argc, char **argv) {
  const char *line;
  string configFile = "";
  Client *hypertable;
  queue<string> command_queue;
  const char *base, *ptr;
  HqlCommandInterpreter *interp;

  System::initialize(argv[0]);
  ReactorFactory::initialize((uint16_t)System::get_processor_count());

  for (int i=1; i<argc; i++) {
    if (!strcmp(argv[i], "--batch") || !strcmp(argv[i], "-B"))
      configFile = &argv[i][9];
    else if (!strncmp(argv[i], "--config=", 9))
      configFile = &argv[i][9];
    else
      Usage::dump_and_exit(usage);
  }

  if (configFile == "")
    configFile = System::installDir + "/conf/hypertable.cfg";

  hypertable = new Client(configFile);

  interp = hypertable->create_hql_interpreter();

  cout << "Welcome to the HQL command interpreter." << endl;
  cout << "Type 'help' for a description of commands." << endl;
  cout << endl << flush;

  try {

    g_accum = "";
    if (!g_batch_mode)
      using_history();
    while ((line = rl_gets()) != 0) {

      if (*line == 0)
	continue;

      /**
       * Add commands to queue
       */
      base = line;
      ptr = strchr(base, ';');
      while (ptr) {
	g_accum += string(base, ptr-base);
	if (g_accum.size() > 0) {
	  boost::trim(g_accum);
	  command_queue.push(g_accum);
	  g_accum = "";
	  g_cont = false;
	}
	base = ptr+1;
	ptr = strchr(base, ';');
      }
      g_accum += string(base);
      boost::trim(g_accum);
      if (g_accum != "")
	g_cont = true;
      g_accum += " ";

      while (!command_queue.empty()) {
	if (command_queue.front() == "quit" || command_queue.front() == "exit")
	  return 0;
	interp->execute_line(command_queue.front());
	command_queue.pop();
      }

    }

  }
  catch (Hypertable::Exception &e) {
    cerr << e.what() << endl;
    return 1;
  }

  return 0;
}
