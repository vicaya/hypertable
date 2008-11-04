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
#include "Common/System.h"
#include "Common/Usage.h"

#include "Global.h"
#include "Hyperspace/Config.h"
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
using namespace Config;
using namespace Hyperspace;
using namespace std;
using namespace boost;


namespace {

  struct AppPolicy : Config::Policy {
    static void init_options() {
      cmdline_desc().add_options()
        ("debug", "Turn on debugging output")
        ("eval,e", str(), "Evaluates the commands in the string <cmds>.  "
            "Several commands can be supplied in <cmds> by separating them "
            "with semicolons")
        ("no-prompt", "Don't display a command prompt")
        ("test-mode", "Suppress file line number information from error "
            "messages to simplify diff'ing test output.")
        ("notification-address", str(), "Notification address for testing")
        ;
      alias("debug", "verbose");
    }
  };

  typedef Meta::list<AppPolicy, HyperspaceClientPolicy, DefaultCommPolicy>
          Policies;

  char *line_read = 0;
  bool g_testmode = false;
  String g_input_str;

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

  typedef std::vector<InteractiveCommand *> Commands;
  Commands commands;

  void init_commands(Session *session) {
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
  }

  int eval_command(const char *line) {
    foreach(InteractiveCommand *command, commands) {
      if (command->matches(line)) {
        command->parse_command_line(line);
        command->run();
        return 0;
      }
    }
    cerr <<"Unrecognized command : "<< line << endl;
    return -1;
  }

  int eval_commands(const String &evals) {
    std::vector<String> strs;
    split(strs, evals, is_any_of(";"));

    foreach(String cmd, strs) {
      Global::exit_status = 0;
      trim(cmd);

      if (eval_command(cmd.c_str()) == -1)
        return -1;
    }
    return Global::exit_status;
  }

} // local namespace


class Notifier {
public:
  Notifier(const char *addr_str) {
    DispatchHandlerPtr null_handler(0);
    m_comm = Comm::instance();
    if (!InetAddr::initialize(&m_addr, addr_str)) {
      exit(1);
    }
    InetAddr::initialize(&m_send_addr, INADDR_ANY, 0);
    m_comm->create_datagram_receive_socket(&m_send_addr, 0x10, null_handler);
  }

  Notifier() : m_comm(0) {
    return;
  }

  void notify() {
    if (m_comm) {
      int error;
      CommBufPtr cbp(new CommBuf(m_builder, 2));
      cbp->append_i16(0);
      if ((error = m_comm->send_datagram(m_addr, m_send_addr, cbp))
          != Error::OK) {
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


class SessionHandler : public SessionCallback {
public:
  virtual void jeopardy() { cout << "SESSION CALLBACK: Jeopardy" << endl; }
  virtual void safe() { cout << "SESSION CALLBACK: Safe" << endl; }
  virtual void expired() { cout << "SESSION CALLBACK: Expired" << endl; }
};


int main(int argc, char **argv, char **envp) {
  try {
    init_with_policies<Policies>(argc, argv);

    int timeout = get_i32("Hyperspace.Timeout");
    String eval = get("eval", String());
    String notifier_str= get("notification-address", String());
    Notifier *notifier = notifier_str.empty()
        ? new Notifier() : new Notifier(notifier_str.c_str());

    if (has("test-mode")) {
      Logger::set_test_mode("hyperspace");
      g_testmode = true;
    }

    Comm *comm = Comm::instance();
    SessionHandler session_handler;
    Session *session = new Session(comm, properties, &session_handler);

    if (!session->wait_for_connection(timeout)) {
      cerr << "Unable to establish session with Hyerspace, exiting..." << endl;
      exit(1);
    }

    init_commands(session);

    if (eval.length())
      return eval_commands(eval);

    cout << "Welcome to the Hyperspace command interpreter.  Hyperspace\n"
         << "is a global namespace and lock service for loosely-coupled\n"
         << "distributed systems.  Type 'help' for a description of commands.\n"
         << endl;

    const char *line;
    using_history();

    while ((line = rl_gets()) != 0) {
      Global::exit_status = 0;

      if (*line == 0)
        continue;

      if (!strcmp(line, "quit") || !strcmp(line, "exit")) {
        notifier->notify();
        exit(0);
      }
      else if (!strncmp(line, "echo", 4)) {
        String echo_str = String(line);
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
        for (size_t i=0; i<commands.size(); i++) {
          Usage::dump(commands[i]->usage());
          cout << endl;
        }
        Usage::dump(help_trailer);
        notifier->notify();
      }
      else {
        try { eval_command(line); }
        catch (Exception &e) {
          HT_ERROR_OUT<< e.what() <<" - "<< Error::get_text(e.code()) <<HT_END;
        }
        notifier->notify();
      }
    }
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    return 1;
  }
  return Global::exit_status;
}
