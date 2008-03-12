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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <string>


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


std::string CommandShell::ms_history_file = "";

namespace {
  void termination_handler (int signum) {
    write_history(CommandShell::ms_history_file.c_str());
    exit(1);
  }

  const char *help_text = 
  "\n" \
  "For information about Hypertable, visit http://www.hypertable.org/\n" \
  "\n" \
  "Interpreter Commands\n" \
  "--------------------\n" \
  "?          (\\?) Synonym for `help'.\n" \
  "clear      (\\c) Clear command.\n" \
  "exit       (\\q) Exit program. Same as quit.\n" \
  "print      (\\p) Print current command.\n" \
  "quit       (\\q) Quit hypertable.\n" \
  "source <f> (.)  Execute commands in file <f>.\n" \
  "system     (\\!) Execute a system shell command.\n" \
  "\n";
}



/**
 */
CommandShell::CommandShell(std::string program_name, CommandInterpreterPtr &interp_ptr, po::variables_map &vm) : m_program_name(program_name), m_interp_ptr(interp_ptr), m_varmap(vm), g_batch_mode(false), g_no_prompt(false), g_cont(false), line_read(0) {
}



/**
 */
char *CommandShell::rl_gets () {
  if (line_read) {
    free (line_read);
    line_read = (char *)NULL;
  }

  /* Get a line from the user. */
  if (g_batch_mode || g_no_prompt) {
    if (!getline(cin, gInputStr))
      return 0;
    boost::trim(gInputStr);
    if (gInputStr.find("quit", 0) != 0)
      cout << gInputStr << endl;
    return (char *)gInputStr.c_str();
  }
  else if (!g_cont)
    line_read = readline("hypertable> ");
  else
    line_read = readline("         -> ");

  /* If the line has any text in it, save it on the history. */
  if (!g_batch_mode && line_read && *line_read)
    add_history (line_read);

  return line_read;
}



void CommandShell::add_options(po::options_description &desc) {
  desc.add_options()
    ("batch", "Disable interactive behavior")
    ("config", po::value<std::string>(), "Configuration file.  The default config file is \"conf/hypertable.cfg\" relative to the toplevel install directory")
    ("no-prompt", "Do not display an input prompt")
    ("timestamp-format", po::value<std::string>(), "Output format for timestamp.  Currently the only formats are 'default' and 'usecs'")
    ;
}


/**
 *
 */
int CommandShell::run() {
  const char *line;
  queue<string> command_queue;
  std::string command;
  std::string timestamp_format;
  std::string source_commands;
  const char *base, *ptr;

  ms_history_file = (std::string)getenv("HOME") + "/." + m_program_name + "_history";

  g_batch_mode = m_varmap.count("batch") ? true : false;
  g_no_prompt = m_varmap.count("no-prompt") ? true : false;
  if (m_varmap.count("timestamp-format"))
    timestamp_format = m_varmap["timestamp-format"].as<string>();

  if (timestamp_format != "")
    m_interp_ptr->set_timestamp_output_format(timestamp_format);

  if (!g_batch_mode) {

    read_history(ms_history_file.c_str());

    cout << "Welcome to the " << m_program_name << " command interpreter." << endl;
    cout << endl;
    cout << "Type 'help;' or '\\h' for help. Type '\\c' to clear the buffer." << endl;
    cout << endl << flush;
  }

  if (signal (SIGINT, termination_handler) == SIG_IGN)
    signal (SIGINT, SIG_IGN);
  if (signal (SIGHUP, termination_handler) == SIG_IGN)
    signal (SIGHUP, SIG_IGN);
  if (signal (SIGTERM, termination_handler) == SIG_IGN)
    signal (SIGTERM, SIG_IGN);

  g_accum = "";
  if (!g_batch_mode)
    using_history();
  while ((line = rl_gets()) != 0) {

    try {

      if (*line == 0)
	continue;

      if (!strncasecmp(line, "help", 4) || !strncmp(line, "\\h", 2) || *line == '?') {
	command = line;
	std::transform( command.begin(), command.end(), command.begin(), ::tolower );
	trim_if(command, boost::is_any_of(" \t\n\r;"));
	if (command == "help" || command == "\\h" || command == "?")
	  cout << help_text;
	m_interp_ptr->execute_line(command);
	continue;
      }
      else if (!strcasecmp(line, "quit") || !strcasecmp(line, "exit") || !strcmp(line, "\\q")) {
	if (!g_batch_mode)
	  write_history(ms_history_file.c_str());
	return 0;
      }
      else if (!strcasecmp(line, "print") || !strcmp(line, "\\p")) {
	cout << g_accum << endl;
	continue;
      }
      else if (!strcasecmp(line, "clear") || !strcmp(line, "\\c")) {
	g_accum = "";
	g_cont = false;
	continue;
      }
      else if (!strncmp(line, "source", 6) || line[0] == '.') {
	if ((base = strchr(line, ' ')) == 0) {
	  cout << "syntax error: source or '.' must be followed by a space character" << endl;
	  continue;
	}
	std::string fname = base;
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
	std::string command = line;
	size_t offset = command.find_first_of(' ');
	if (offset != std::string::npos) {
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
	g_accum += string(base, ptr-base);
	if (g_accum.size() > 0) {
	  boost::trim(g_accum);
	  if (g_accum.find("#") != 0)
	    command_queue.push(g_accum);
	  g_accum = "";
	  g_cont = false;
	}
	base = ptr+1;
	ptr = strchr(base, ';');
      }
      command = string(base);
      boost::trim(command);
      if (command != "" && command.find("#") != 0) {
	g_accum += command;
	boost::trim(g_accum);
      }
      if (g_accum != "") {
	g_cont = true;
	g_accum += " ";
      }

      while (!command_queue.empty()) {
	if (command_queue.front() == "quit" || command_queue.front() == "exit") {
	  if (!g_batch_mode)
	    write_history(ms_history_file.c_str());
	  return 0;
	}
	command = command_queue.front();
	command_queue.pop();
	if (!strncmp(command.c_str(), "pause", 5)) {
	  std::string sec_str = command.substr(5);
	  boost::trim(sec_str);
	  char *endptr;
	  long secs = strtol(sec_str.c_str(), &endptr, 0);
	  if (secs == 0 && errno == EINVAL || *endptr != 0) {
	    cout << "error: invalid seconds specification" << endl;
	    if (g_batch_mode)
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
      if (g_batch_mode)
	return 1;
      g_accum = "";
      while (!command_queue.empty())
	command_queue.pop();
      g_cont=false;
    }

  }

  if (!g_batch_mode)
    write_history(ms_history_file.c_str());
  return 0;

}

