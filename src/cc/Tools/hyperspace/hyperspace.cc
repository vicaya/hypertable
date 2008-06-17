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
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/HeaderBuilder.h"
#include "Common/Error.h"
#include "Common/InetAddr.h"
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
#include "CommandCreate.h"
#include "CommandClose.h"
#include "CommandAttrSet.h"
#include "CommandAttrGet.h"
#include "CommandAttrDel.h"
#include "CommandExists.h"
#include "CommandReaddir.h"
#include "CommandLock.h"
#include "CommandTryLock.h"
#include "CommandRelease.h"
#include "CommandGetSequencer.h"

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;


namespace {

  char *line_read = 0;

  bool g_testmode = false;
  std::string g_input_str;

  char *rl_gets () {

    if (g_testmode) {
      if (!getline(cin, g_input_str))
        return 0;
      boost::trim(g_input_str);
      if (g_input_str.find("echo", 0) != 0 && g_input_str.find("quit", 0) != 0)
        cout << g_input_str << endl;
      return (char *)g_input_str.c_str();
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
    "  --notification-address=[<host>:]<port>  Send notification datagram to this",
    "                   address after each command.",
    "  --help           Display this help text and exit",
    "  --test-mode      Suppress file line number information from error messages to",
    "                   simplify diff'ing test output.",
    "",
    "This is a command interpreter for Hyperspace, a global namespace and lock service",
    "for loosely-coupled distributed systems.",
    (const char *)0
  };

  const char *help_trailer[] = {
    "echo <str>",
    "  Display <str> to stdout.",
    "",
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
class Notifier {

public:

  Notifier(const char *addr_str) {
    DispatchHandlerPtr null_handler(0);
    int error;
    m_comm = new Comm();
    if (!InetAddr::initialize(&m_addr, addr_str)) {
      exit(1);
    }
    InetAddr::initialize(&m_send_addr, INADDR_ANY, 0);
    if ((error = m_comm->create_datagram_receive_socket(&m_send_addr, null_handler)) != Error::OK) {
      std::string str;
      HT_ERRORF("Problem creating UDP receive socket %s - %s", InetAddr::string_format(str, m_send_addr), Error::get_text(error));
      exit(1);
    }
  }

  Notifier() : m_comm(0) {
    return;
  }

  void notify() {
    if (m_comm) {
      int error;
      CommBufPtr cbp(new CommBuf(m_builder, 2));
      cbp->append_i16(0);
      if ((error = m_comm->send_datagram(m_addr, m_send_addr, cbp)) != Error::OK) {
        HT_ERRORF("Problem sending datagram - %s", Error::get_text(error));
        exit(1);
      }
    }
  }

private:
  Comm *m_comm;
  struct sockaddr_in m_addr;
  struct sockaddr_in m_send_addr;
  HeaderBuilder m_builder;
};


/**
 *
 */
class SessionHandler : public SessionCallback {
public:
  virtual void jeopardy() { cout << "SESSION CALLBACK: Jeopardy" << endl << flush; }
  virtual void safe() { cout << "SESSION CALLBACK: Safe" << endl << flush; }
  virtual void expired() { cout << "SESSION CALLBACK: Expired" << endl << flush; }
};



/**
 *  main
 */
int main(int argc, char **argv, char **envp) {
  const char *line;
  char *eval = 0;
  size_t i;
  string cfgfile = "";
  vector<InteractiveCommand *>  commands;
  Comm *comm;
  PropertiesPtr props_ptr;
  Hyperspace::Session *session;
  SessionHandler session_handler;
  bool verbose = false;
  int error;
  Notifier *notifier = 0;

  System::initialize(argv[0]);
  ReactorFactory::initialize((uint16_t)System::get_processor_count());

  cfgfile = System::install_dir + "/conf/hypertable.cfg";

  for (int i=1; i<argc; i++) {
    if (!strncmp(argv[i], "--config=", 9))
      cfgfile = &argv[i][9];
    else if (!strcmp(argv[i], "--debug"))
      verbose = true;
    else if (!strcmp(argv[i], "--eval")) {
      i++;
      if (i == argc)
        Usage::dump_and_exit(usage);
      eval = argv[i];
    }
    else if (!strcmp(argv[i], "--test-mode")) {
      Logger::set_test_mode("hyperspace");
      g_testmode = true;
    }
    else if (!strncmp(argv[i], "--notification-address=", 23))
      notifier = new Notifier(&argv[i][23]);
    else
      Usage::dump_and_exit(usage);
  }

  if (notifier == 0)
    notifier = new Notifier();

  props_ptr = new Properties(cfgfile);

  if (verbose)
    props_ptr->set("Hypertable.Verbose", "true");

  comm = new Comm();

  session = new Session(comm, props_ptr, &session_handler);

  if (!session->wait_for_connection(30)) {
    cerr << "Unable to establish session with Hyerspace, exiting..." << endl;
    exit(1);
  }

  commands.push_back(new CommandMkdir(session));
  commands.push_back(new CommandDelete(session));
  commands.push_back(new CommandOpen(session));
  commands.push_back(new CommandCreate(session));
  commands.push_back(new CommandClose(session));
  commands.push_back(new CommandAttrSet(session));
  commands.push_back(new CommandAttrGet(session));
  commands.push_back(new CommandAttrDel(session));
  commands.push_back(new CommandExists(session));
  commands.push_back(new CommandReaddir(session));
  commands.push_back(new CommandLock(session));
  commands.push_back(new CommandTryLock(session));
  commands.push_back(new CommandRelease(session));
  commands.push_back(new CommandGetSequencer(session));

  /**
   * Non-interactive mode
   */
  if (eval != 0) {
    const char *str;
    std::string cmd_str;
    str = strtok(eval, ";");
    while (str) {
      Global::exit_status = 0;
      cmd_str = str;
      boost::trim(cmd_str);
      for (i=0; i<commands.size(); i++) {
        if (commands[i]->matches(cmd_str.c_str())) {
          commands[i]->parse_command_line(cmd_str.c_str());
          if ((error = commands[i]->run()) != Error::OK) {
            cerr << Error::get_text(error) << endl;
            return 1;
          }
          break;
        }
      }
      if (i == commands.size()) {
        HT_ERRORF("Unrecognized command : %s", cmd_str.c_str());
        return 1;
      }
      str = strtok(0, ";");
    }
    return Global::exit_status;
  }

  cout << "Welcome to the Hyperspace command interpreter.  Hyperspace" << endl;
  cout << "is a global namespace and lock service for loosely-coupled" << endl;
  cout << "distributed systems.  Type 'help' for a description of commands." << endl;
  cout << endl << flush;

  using_history();
  while ((line = rl_gets()) != 0) {

    Global::exit_status = 0;

    if (*line == 0)
      continue;

    for (i=0; i<commands.size(); i++) {
      if (commands[i]->matches(line)) {
        commands[i]->parse_command_line(line);
        if ((error = commands[i]->run()) != Error::OK && error != -1)
          cout << Error::get_text(error) << endl;
        notifier->notify();
        break;
      }
    }

    if (i == commands.size()) {
      if (!strcmp(line, "quit") || !strcmp(line, "exit")) {
        notifier->notify();
        exit(0);
      }
      else if (!strncmp(line, "echo", 4)) {
        std::string echo_str = std::string(line);
        echo_str = echo_str.substr(4);
        boost::trim_if(echo_str, boost::is_any_of("\" \t"));
        cout << echo_str << endl;
        notifier->notify();
      }
      else if (!strcmp(line, "pwd")) {
        cout << Global::cwd << endl;
        notifier->notify();
      }
      else if (!strcmp(line, "help")) {
        cout << endl;
        for (i=0; i<commands.size(); i++) {
          Usage::dump(commands[i]->usage());
          cout << endl;
        }
        Usage::dump(help_trailer);
        notifier->notify();
      }
      else {
        cout << "Unrecognized command." << endl;
        notifier->notify();
      }
    }
  }

  delete session;
  delete comm;
  return Global::exit_status;
}
