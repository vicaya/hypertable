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

#include <cstring>
#include <iostream>

#include "Common/Error.h"
#include "Common/System.h"

#include "Hypertable/Lib/ApacheLogParser.h"
#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/KeySpec.h"

using namespace Hypertable;
using namespace std;

namespace {

  /** 
   * This function returns a null terminated pointer to the page that is
   * referenced in the given GET request.
   */
  const char *extract_requested_page(char *request) {
    char *base, *ptr;
    if (!strncmp(request, "GET ", 4)) {
      base = request + 4;
      if ((ptr = strchr(base, ' ')) != 0)
	*ptr = 0;
      return base;
    }
    return 0;
  }

  /** 
   * This function extracts the toplevel User agent from a user agent string,
   * which involves stripping off the version specific info in parenthesis
   */
  const char *extract_toplevel_user_agent(char *user_agent) {
    char *ptr;
    if (user_agent) {
      if ((ptr = strchr(user_agent, '(')) != 0)
	*ptr = 0;
      return user_agent;
    }
    return "-";
  }

}


/**
 * This program is designed to parse an Apache web server log and insert
 * the values into a table called 'WebServerLog'.  The name of the Apache
 * web server log file is supplied as the one and only argument on the
 * command line.  The expected format of the log is Apache Common or
 * Combined format (see http://httpd.apache.org/docs/1.3/logs.html#common
 * for details).
 */
int main(int argc, char **argv) {
  int error;
  ApacheLogParser parser;
  ApacheLogEntryT entry;
  ClientPtr hypertable_client_ptr;
  TablePtr table_ptr;
  TableMutatorPtr mutator_ptr;
  KeySpec key;

  if (argc <= 1) { 
    cout << "usage: apache_log_load <file1>" << endl;
    return 0;
  }

  try {

    // Create Hypertable client object
    hypertable_client_ptr = new Hypertable::Client(argv[0]);

    // Open the 'WebServerLog' table
    if ((error = hypertable_client_ptr->open_table("WebServerLog", table_ptr)) != Error::OK) {
      cerr << "Error: unable to open table 'WebServerLog' - " << Error::get_text(error) << endl;
      return 1;
    }

    // Create a mutator object on the 'WebServerLog' table
    if ((error = table_ptr->create_mutator(mutator_ptr)) != Error::OK) {
      cerr << "Error: problem creating mutator on table 'WebServerLog' - " << Error::get_text(error) << endl;
      return 1;
    }

  }
  catch (std::exception &e) {
    cerr << "error: " << e.what() << endl;
    return 1;
  }

  // Initialize the key variable, 'RequestInfo' is the only column we're updating
  memset(&key, 0, sizeof(key));
  key.column_family = "RequestInfo";

  // Load the log file into the ApacheLogParser object
  parser.load(argv[1]);

  // The parser next method will return true until EOF
  while (parser.next(entry)) {

    // Extract the page being requested and use it as the row key
    if ((key.row = extract_requested_page(entry.request)) == 0)
      continue;
    key.row_len = strlen((const char *)key.row);

    // Assemble value:  <IpAddress> <Referer> <UserAgent>
    try {
      std::string str;
      str += (entry.ip_address) ? entry.ip_address : "-";
      str += " ";
      str += (entry.referer) ? entry.referer : "-";
      str += " ";
      str += extract_toplevel_user_agent(entry.user_agent);
      mutator_ptr->set(entry.timestamp, key, str.c_str(), str.length());
    }
    catch (Hypertable::Exception &e) {
      cerr << "error: " << Error::get_text(e.code()) << " - " << e.what() << endl;
    }

  }

  // Flush pending updates
  try {
    mutator_ptr->flush();
  }
  catch (Hypertable::Exception &e) {
    cerr << "error: " << Error::get_text(e.code()) << " - " << e.what() << endl;
    return 1;
  }

  return 0;
}
