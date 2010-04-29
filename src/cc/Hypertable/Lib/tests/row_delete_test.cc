/** -*- c++ -*-
 * Copyright (C) 2010 Doug Judd (Hypertable, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of the
 * License, or any later version.
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
#include <iostream>

#include "Common/md5.h"
#include "Common/Usage.h"

#include "Hypertable/Lib/Client.h"

using namespace std;
using namespace Hypertable;

namespace {

  const char *schema =
  "<Schema>"
  "  <AccessGroup name=\"default\">"
  "    <ColumnFamily>"
  "      <Name>data</Name>"
  "    </ColumnFamily>"
  "  </AccessGroup>"
  "</Schema>";

  const char *usage[] = {
    "usage: row_delete_test [<seed>]",
    "",
    "Validates the insertion and retrieval of large items.",
    0
  };


}


int main(int argc, char **argv) {
  unsigned long seed = 1234;

  if (argc > 2 ||
      (argc == 2 && (!strcmp(argv[1], "--help") || !strcmp(argv[1], "-?"))))
    Usage::dump_and_exit(usage);

  if (argc == 2)
    seed = atoi(argv[1]);

  srandom(seed);

  try {
    Client *hypertable = new Client(argv[0], "./hypertable.cfg");
    TablePtr table_ptr;
    TableMutatorPtr mutator_ptr;
    TableScannerPtr scanner_ptr;
    KeySpec key;
    Cell cell;
    const char *value1 = "Hello, World! (1)";
    const char *value2 = "Hello, World! (2)";
    const char *expected[] = { "foo : DELETE ROW", "foo data: Hello, World! (2)" };

    /**
     * Validate large object returned by CREATE_SCANNER
     */
    hypertable->drop_table("RowDeleteTest", true);
    hypertable->create_table("RowDeleteTest", schema);

    table_ptr = hypertable->open_table("RowDeleteTest");

    mutator_ptr = table_ptr->create_mutator();

    key.row = "foo";
    key.row_len = strlen("foo");
    key.column_family = "data";
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;
    mutator_ptr->set(key, value1, strlen(value1));
    mutator_ptr->flush();

    key.row = "foo";
    key.row_len = strlen("foo");
    key.column_family = 0;
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;
    mutator_ptr->set_delete(key);
    mutator_ptr->flush();

    key.row = "foo";
    key.row_len = strlen("foo");
    key.column_family = "data";
    key.column_qualifier = 0;
    key.column_qualifier_len = 0;
    mutator_ptr->set(key, value2, strlen(value2));
    mutator_ptr->flush();

    mutator_ptr = 0;

    ScanSpec scan_spec;

    scan_spec.return_deletes = true;
    scanner_ptr = table_ptr->create_scanner(scan_spec);

    std::vector<String>  values;
    String result;
    while (scanner_ptr->next(cell)) {
      result = String(cell.row_key);
      if (cell.column_family) {
	result += String(" ") + cell.column_family;
	if (cell.column_qualifier)
	  result += String(":") + cell.column_qualifier;
      }
      if (cell.flag == FLAG_DELETE_ROW)
	result += String(" DELETE ROW");
      else if (cell.flag == FLAG_DELETE_COLUMN_FAMILY)
	result += String(" DELETE COLUMN FAMILY");
      else if (cell.flag == FLAG_DELETE_CELL)
	result += String(" DELETE CELL");
      else
	result += String(" ") + String((const char *)cell.value, cell.value_len);
      values.push_back(result);
    }

    for (size_t i=0; i<values.size(); i++) {
      if (values[i] != String(expected[i])) {
	std::cout << "Expected:\n";
	std::cout << expected[0] << "\n" << expected[1] << "\n\n";
	std::cout << "Got:\n";
	for (size_t j=0; j<values.size(); j++)
	  std::cout << values[i] << "\n";
	std::cout << endl;
	_exit(1);
      }
    }

    scanner_ptr = 0;
    table_ptr = 0;
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
    _exit(1);
  }

  _exit(0);
}
