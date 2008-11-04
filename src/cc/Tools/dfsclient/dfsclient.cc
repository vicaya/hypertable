/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include <cstring>
#include <iostream>
#include <vector>

extern "C" {
#include <readline/readline.h>
#include <readline/history.h>
}

#include <boost/algorithm/string.hpp>
#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "Common/Error.h"
#include "Common/InteractiveCommand.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"
#include "AsyncComm/Comm.h"

#include "CommandCopyFromLocal.h"
#include "CommandCopyToLocal.h"
#include "CommandLength.h"
#include "CommandMkdirs.h"
#include "CommandRemove.h"
#include "CommandRmdir.h"
#include "CommandShutdown.h"
#include "CommandExists.h"

using namespace Hypertable;
using namespace Config;
using namespace std;
using namespace boost;

namespace {

  struct MyPolicy : Policy {
    static void init_options() {
      cmdline_desc("Usage: %s [Options]\n\n"
        "This is a command line interface to the DFS broker.\n\n"
        "Options").add_options()
        ("eval,e", str(), "Evalute semicolon separted commands")
        ;
    }
  };

  typedef Meta::list<MyPolicy, DfsClientPolicy, DefaultCommPolicy> Policies;

  char *line_read = 0;

  char *rl_gets () {

    if (line_read) {
      free (line_read);
      line_read = (char *)NULL;
    }

    /* Get a line from the user. */
    line_read = readline ("dfsclient> ");

    /* If the line has any text in it, save it on the history. */
    if (line_read && *line_read)
      add_history (line_read);

    return line_read;
  }

} // local namespace


int main(int argc, char **argv) {
  try {
    init_with_policies<Policies>(argc, argv);

    String host = get_str("DfsBroker.Host");
    uint16_t port = get_i16("DfsBroker.Port");
    int timeout = get_i32("timeout");

    DfsBroker::Client *client = new DfsBroker::Client(host, port, timeout);

    vector<InteractiveCommand *>  commands;
    commands.push_back(new CommandCopyFromLocal(client));
    commands.push_back(new CommandCopyToLocal(client));
    commands.push_back(new CommandLength(client));
    commands.push_back(new CommandMkdirs(client));
    commands.push_back(new CommandRemove(client));
    commands.push_back(new CommandRmdir(client));
    commands.push_back(new CommandShutdown(client));
    commands.push_back(new CommandExists(client));

    /**
     * Non-interactive mode
     */
    String eval = get_str("eval", String());
    size_t i;

    if (eval.length()) {
      std::vector<String> strs;
      split(strs, eval, is_any_of(";"));

      foreach(String cmd, strs) {
        trim(cmd);

        for (i=0; i<commands.size(); i++) {
          if (commands[i]->matches(cmd.c_str())) {
            commands[i]->parse_command_line(cmd.c_str());
            commands[i]->run();
            break;
          }
        }
        if (i == commands.size()) {
          HT_ERRORF("Unrecognized command : %s", cmd.c_str());
          return 1;
        }
      }
      return 0;
    }

    cout <<"Welcome to dsftool, a command-line interface to the DFS broker.\n"
         <<"Type 'help' for a description of commands.\n" << endl;

    using_history();
    const char *line;

    while ((line = rl_gets()) != 0) {
      if (*line == 0)
        continue;

      for (i=0; i<commands.size(); i++) {
        if (commands[i]->matches(line)) {
          commands[i]->parse_command_line(line);
          commands[i]->run();
          break;
        }
      }

      if (i == commands.size()) {
        if (!strcmp(line, "quit") || !strcmp(line, "exit"))
          exit(0);
        else if (!strcmp(line, "help")) {
          cout << endl;
          for (i=0; i<commands.size(); i++) {
            Usage::dump(commands[i]->usage());
            cout << endl;
          }
        }
        else
          cout << "Unrecognized command." << endl;
      }

    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return 0;
}
