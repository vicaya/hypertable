/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License.
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
#include <cstring>
#include <iostream>
#include <queue>

extern "C" {
#include <errno.h>
#include <limits.h>
#include <poll.h>
#include <readline/history.h>
#include <readline/readline.h>
#include <signal.h>
}

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/thread/exceptions.hpp>

#include "Common/Error.h"
#include "Common/FileUtils.h"
#include "Common/Properties.h"
#include "Common/System.h"
#include "Common/Usage.h"

#include "CommandShell.h"

using namespace Hypertable;
using namespace std;


String CommandShell::ms_history_file = "";

namespace {
  void termination_handler (int signum) {
    write_history(CommandShell::ms_history_file.c_str());
    exit(1);
  }

  const char *help_text =
  "\n" \
  "Interpreter Meta Commands\n" \
  "-------------------------\n" \
  "?          (\\?) Synonym for `help'.\n" \
  "clear      (\\c) Clear command.\n" \
  "exit       (\\q) Exit program. Same as quit.\n" \
  "print      (\\p) Print current command.\n" \
  "quit       (\\q) Quit program.\n" \
  "source <f> (.)  Execute commands in file <f>.\n" \
  "system     (\\!) Execute a system shell command.\n" \
  "\n";
}



/**
 */
CommandShell::CommandShell(const String &program_name,
    CommandInterpreterPtr &interp_ptr, const Config::VarMap &vm)
    : m_program_name(program_name), m_interp_ptr(interp_ptr), m_varmap(vm),
      m_batch_mode(false), m_silent(false), m_test_mode(false),
      m_no_prompt(false), m_cont(false), m_line_read(0) {
  m_prompt_str = program_name + "> ";
  m_batch_mode = m_varmap.count("batch") ? true : false;
  if (m_batch_mode)
    m_silent = true;
  else
    m_silent = m_varmap.count("silent") ? true : false;
  m_test_mode = m_varmap.count("test-mode") ? true : false;
  m_no_prompt = m_varmap.count("no-prompt") ? true : false;
}



/**
 */
char *CommandShell::rl_gets () {
  if (m_line_read) {
    free (m_line_read);
    m_line_read = (char *)NULL;
  }

  /* Get a line from the user. */
  if (m_batch_mode || m_no_prompt || m_silent || m_test_mode) {
    if (!getline(cin, m_input_str))
      return 0;
    boost::trim(m_input_str);
    if (m_input_str.find("quit", 0) != 0 && !m_silent)
      cout << m_input_str << endl;
    return (char *)m_input_str.c_str();
  }
  else if (!m_cont)
    m_line_read = readline(m_prompt_str.c_str());
  else
    m_line_read = readline("         -> ");

  /* If the line has any text in it, save it on the history. */
  if (!m_batch_mode && !m_test_mode && m_line_read && *m_line_read)
    add_history (m_line_read);

  return m_line_read;
}



void CommandShell::add_options(Config::Desc &desc) {
  desc.add_options()
    ("batch", "Disable interactive behavior")
    ("no-prompt", "Do not display an input prompt")
    ("silent", "Be silent. Don't echo commands or display progress")
    ("test-mode", "Don't display anything that might change from run to run (e.g. timing statistics)")
    ("timestamp-format", Config::value<std::string>(), "Output format for timestamp.  Currently the only formats are 'default' and 'usecs'")
    ;
}


/**
 *
 */
int CommandShell::run() {
  const char *line;
  queue<string> command_queue;
  String command;
  String timestamp_format;
  String source_commands;
  const char *base, *ptr;

  ms_history_file = (String)getenv("HOME") + "/." + m_program_name + "_history";

  if (m_varmap.count("timestamp-format"))
    timestamp_format = m_varmap["timestamp-format"].as<String>();

  if (timestamp_format != "")
    m_interp_ptr->set_timestamp_output_format(timestamp_format);

  if (!m_batch_mode && !m_silent) {

    read_history(ms_history_file.c_str());

    cout << endl;
    cout << "Welcome to the " << m_program_name << " command interpreter." << endl;
    cout << "For information about Hypertable, visit http://www.hypertable.org/" << endl;
    cout << endl;
    cout << "Type 'help' for a list of commands, or 'help shell' for a" << endl;
    cout << "list of shell meta commands." << endl;
    cout << endl << flush;
  }

  if (signal (SIGINT, termination_handler) == SIG_IGN)
    signal (SIGINT, SIG_IGN);
  if (signal (SIGHUP, termination_handler) == SIG_IGN)
    signal (SIGHUP, SIG_IGN);
  if (signal (SIGTERM, termination_handler) == SIG_IGN)
    signal (SIGTERM, SIG_IGN);

  m_accum = "";
  if (!m_batch_mode)
    using_history();
  while ((line = rl_gets()) != 0) {

    try {

      if (*line == 0)
        continue;

      if (!strncasecmp(line, "help shell", 10)) {
        cout << help_text;
        continue;
      }
      else if (!strncasecmp(line, "help", 4) || !strncmp(line, "\\h", 2) || *line == '?') {
        command = line;
        std::transform(command.begin(), command.end(), command.begin(), ::tolower);
        trim_if(command, boost::is_any_of(" \t\n\r;"));
        m_interp_ptr->execute_line(command);
        continue;
      }
      else if (!strcasecmp(line, "quit") || !strcasecmp(line, "exit") || !strcmp(line, "\\q")) {
        if (!m_batch_mode)
          write_history(ms_history_file.c_str());
        return 0;
      }
      else if (!strcasecmp(line, "print") || !strcmp(line, "\\p")) {
        cout << m_accum << endl;
        continue;
      }
      else if (!strcasecmp(line, "clear") || !strcmp(line, "\\c")) {
        m_accum = "";
        m_cont = false;
        continue;
      }
      else if (!strncmp(line, "source", 6) || line[0] == '.') {
        if ((base = strchr(line, ' ')) == 0) {
          cout << "syntax error: source or '.' must be followed by a space character" << endl;
          continue;
        }
        String fname = base;
        trim_if(fname, boost::is_any_of(" \t\n\r;"));
        off_t flen;
        if ((base = FileUtils::file_to_buffer(fname.c_str(), &flen)) == 0)
          continue;
        source_commands = "";
        ptr = strtok((char *)base, "\n\r");
        while (ptr != 0) {
          command = ptr;
          boost::trim(command);
          if (command.find("#") != 0)
            source_commands += command + " ";
          ptr = strtok(0, "\n\r");
        }
        if (source_commands == "")
          continue;
        delete [] base;
        line = source_commands.c_str();
      }
      else if (!strncasecmp(line, "system", 6) || !strncmp(line, "\\!", 2)) {
        String command = line;
        size_t offset = command.find_first_of(' ');
        if (offset != String::npos) {
          command = command.substr(offset+1);
          trim_if(command, boost::is_any_of(" \t\n\r;"));
          system(command.c_str());
        }
        continue;
      }
      else if (!strcasecmp(line, "status") || !strcmp(line, "\\s")) {
        cout << endl << "no status." << endl << endl;
        continue;
      }


      /**
       * Add commands to queue
       */
      base = line;
      ptr = strchr(base, ';');
      while (ptr) {
        m_accum += string(base, ptr-base);
        if (m_accum.size() > 0) {
          boost::trim(m_accum);
          if (m_accum.find("#") != 0)
            command_queue.push(m_accum);
          m_accum = "";
          m_cont = false;
        }
        base = ptr+1;
        ptr = strchr(base, ';');
      }
      command = string(base);
      boost::trim(command);
      if (command != "" && command.find("#") != 0) {
        m_accum += command;
        boost::trim(m_accum);
      }
      if (m_accum != "") {
        m_cont = true;
        m_accum += " ";
      }

      while (!command_queue.empty()) {
        if (command_queue.front() == "quit" || command_queue.front() == "exit") {
          if (!m_batch_mode)
            write_history(ms_history_file.c_str());
          return 0;
        }
        command = command_queue.front();
        command_queue.pop();
        if (!strncmp(command.c_str(), "pause", 5)) {
          String sec_str = command.substr(5);
          boost::trim(sec_str);
          char *endptr;
          long secs = strtol(sec_str.c_str(), &endptr, 0);
          if (secs == 0 && errno == EINVAL || *endptr != 0) {
            cout << "error: invalid seconds specification" << endl;
            if (m_batch_mode)
              return 1;
          }
          else
            poll(0, 0, secs*1000);
        }
        else
          m_interp_ptr->execute_line(command);
      }

    }
    catch (Hypertable::Exception &e) {
      cerr << "Error: " << e.what() << " - " << Error::get_text(e.code()) << endl;
      if (m_batch_mode)
        return 1;
      m_accum = "";
      while (!command_queue.empty())
        command_queue.pop();
      m_cont=false;
    }

  }

  if (!m_batch_mode)
    write_history(ms_history_file.c_str());
  return 0;

}

