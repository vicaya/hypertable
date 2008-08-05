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
#include "Common/Compat.h"

#include <cstdio>
#include <cstring>
#include <iostream>

#include "Common/Error.h"
#include "Common/System.h"

#include "Hypertable/Lib/Client.h"
#include "Hypertable/Lib/KeySpec.h"

#include "freebase_parser.h"

using namespace Hypertable;
using namespace std;

namespace {

  const char *usage = 
    "\n"
    "  usage: freebase_load <file>\n"
    "\n"
    "  Loads a freebase .tsv file into a table called\n"
    "  freebase with the following schema:\n"
    "\n"
    "create table freebase (\n"
    "  name,\n"
    "  category,\n"
    "  property\n"
    ");"
    "\n"
    "  This program assumes that the first line in the\n"
    "  .tsv file contains the column names and that the\n"
    "  first two columns are 'name' followed by 'id'.\n"
    "  The 'id' column is used as the row key and the\n"
    "  name of the file (minus the .tsv extension) is\n"
    "  taken as the category.\n";

  const int RETRY_TIMEOUT = 30;

}



/**
 *
 */
int main(int argc, char **argv) {
  freebase_parser parser;
  ClientPtr client_ptr;
  TablePtr table_ptr;
  TableMutatorPtr mutator_ptr;
  KeySpec key;
  const char *inputfile;
  String row;
  InsertRec *recs;
  int count;

  if (argc == 2)
    inputfile = argv[1];
  else {
    cout << usage << endl;
    return 0;
  }

  parser.load(inputfile);

  try {

    // Create Hypertable client object
    client_ptr = new Client( System::locate_install_dir(argv[0]) );

    // Open the 'free' table
    table_ptr = client_ptr->open_table("freebase");

    // Create a mutator object on the
    // 'LogDb' table
    mutator_ptr = table_ptr->create_mutator();

  }
  catch (Exception &e) {
    cerr << "Exception caught: " << Error::get_text(e.code()) << " - " << e.what() << endl;
    return 1;
  }

  while ((recs = parser.next(&count)) != 0) {
    for (int i=0; i<count; i++) {
      try {
	mutator_ptr->set(recs[i].key, recs[i].value, recs[i].value_len);
      }
      catch (Exception &e) {
	cerr << "Exception caught: " << Error::get_text(e.code()) << " - " << e.what() << endl;
	_exit(1);
      }
    }
  }

  // Flush pending updates
  try {
    mutator_ptr->flush();
  }
  catch (Exception &e) {
    cerr << "Exception caught: " << Error::get_text(e.code()) << endl;
    _exit(1);
  }

  return 0;
}
