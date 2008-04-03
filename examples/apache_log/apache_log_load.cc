/**
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

#include <cstdio>
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
   * This function returns  the page that is
   * referenced in the given GET request.
   */
  String extract_page(char *request) {
    String retstr;
    const char *base, *ptr;
    if (!strncmp(request, "GET ", 4)) {
      base = request + 4;
      if ((ptr = strchr(base, ' ')) != 0)
	retstr = String(base, ptr-base);
      else
	retstr = base;
    }
    else
      retstr = "-";
    return retstr;
  }

  /**
   * This function displays a textual
   * representation of the given exception
   * to stderr
   */
  void report_error(Exception &e) {
    cerr << "error: "
	 << Error::get_text(e.code())
	 << " - " << e.what() << endl;
  }

  const char *usage = 
    "\n"
    "  usage: apache_log_load [--time-order] <file>\n"
    "\n"
    "  Loads the Apache web log <file> into the LogDb\n"
    "  table.  By default, the row key is constructed\n"
    "  as:\n"
    "\n"
    "  <page> <timestamp>\n"
    "\n"
    "  This format facilitates queries that return\n"
    "  the click history for a specific page.  If\n"
    "  the --time-order switch is supplied, then\n"
    "  the row key is constructed as:\n"
    "\n"
    "  <timestamp> <page>\n"
    "\n"
    "  This format facilitates queries that return\n"
    "  a historical portion of the log.\n";

}



/**
 * This program is designed to parse an Apache web
 * server log and insert the values into a table
 * called 'LogDb'.  By default, the row keys of
 * the table are constructed as follows:
 *
 * <page> <timestamp>
 *
 * This facilitates queries that return the click
 * history for a specific page.  If the
 * --time-order switch is given, then the row keys
 * of the table are constructed as follows:
 *
 * <timestamp> <page>
 *
 * This facilitates queries that return a
 * historical portion of the log.  The expected
 * format of the log is Apache Common or Combined
 * format.  For details, see:
 *
 * http://httpd.apache.org/docs/1.3/logs.html#common
 *
 */
int main(int argc, char **argv) {
  ApacheLogParser parser;
  ApacheLogEntryT entry;
  ClientPtr client_ptr;
  TablePtr table_ptr;
  TableMutatorPtr mutator_ptr;
  KeySpec key;
  const char *inputfile;
  bool time_order = false;
  String row;

  if (argc == 2)
    inputfile = argv[1];
  else if (argc == 3 && !strcmp(argv[1], "--time-order")) {
    time_order = true;
    inputfile = argv[2];
  }
  else {
    cout << usage << endl;
    return 0;
  }

  try {

    // Create Hypertable client object
    client_ptr = new Client(argv[0]);

    // Open the 'LogDb' table
    table_ptr = client_ptr->open_table("LogDb");

    // Create a mutator object on the
    // 'LogDb' table
    mutator_ptr = table_ptr->create_mutator();

  }
  catch (Exception &e) {
    report_error(e);
    return 1;
  }

  memset(&key, 0, sizeof(key));

  // Load the log file into the ApacheLogParser
  // object
  parser.load(inputfile);

  // The parser next method will return true
  // until EOF
  while (parser.next(entry)) {

    // Assemble the row key
    if (time_order) {
      row = format("%d-%02d-%02d %02d:%02d:%02d ", entry.tm.tm_year+1900, entry.tm.tm_mon+1, entry.tm.tm_mday, entry.tm.tm_hour, entry.tm.tm_min, entry.tm.tm_sec);
      row += extract_page(entry.request);
    }
    else {
      row = extract_page(entry.request);
      row += format(" %d-%02d-%02d %02d:%02d:%02d", entry.tm.tm_year+1900, entry.tm.tm_mon+1, entry.tm.tm_mday, entry.tm.tm_hour, entry.tm.tm_min, entry.tm.tm_sec);
    }

    key.row = row.c_str();
    key.row_len = row.length();

    try {
      key.column_family = "ClientIpAddress";
      mutator_ptr->set(key, entry.ip_address);
      key.column_family = "UserId";
      mutator_ptr->set(key, entry.userid);
      key.column_family = "Request";
      mutator_ptr->set(key, entry.request);
      key.column_family = "ResponseCode";
      mutator_ptr->set(key, entry.response_code);
      key.column_family = "ObjectSize";
      mutator_ptr->set(key, entry.object_size);
      key.column_family = "Referer";
      mutator_ptr->set(key, entry.referer);
      key.column_family = "UserAgent";
      mutator_ptr->set(key, entry.user_agent);
    }
    catch (Exception &e) {
      report_error(e);
      return 1;
    }

  }

  // Flush pending updates
  try {
    mutator_ptr->flush();
  }
  catch (Exception &e) {
    report_error(e);
    return 1;
  }

  return 0;
}
