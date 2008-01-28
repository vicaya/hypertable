
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

    hypertable_client_ptr = new Client(argv[0], System::installDir + "/conf/hypertable.cfg");

    if ((error = hypertable_client_ptr->open_table("WebServerLog", table_ptr)) != Error::OK) {
      cerr << "Error: unable to open table 'WebServerLog' - " << Error::get_text(error) << endl;
      return 1;
    }

    if ((error = table_ptr->create_mutator(mutator_ptr)) != Error::OK) {
      cerr << "Error: problem creating mutator on table 'WebServerLog' - " << Error::get_text(error) << endl;
      return 1;
    }

  }
  catch (std::exception &e) {
    cerr << "error: " << e.what() << endl;
    return 1;
  }

  memset(&key, 0, sizeof(key));
  key.column_family = "RequestInfo";

  parser.load(argv[1]);

  while (parser.next(entry)) {

    if (entry.timestamp == 0)
      continue;

    if ((key.row = extract_requested_page(entry.request)) == 0)
      continue;
    key.row_len = strlen((const char *)key.row);

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

  /**
   * Flush pending updates
   */
  try {
    mutator_ptr->flush();
  }
  catch (Hypertable::Exception &e) {
    cerr << "error: " << Error::get_text(e.code()) << " - " << e.what() << endl;
    return 1;
  }

  return 0;
}
