/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
 * 
 * This file is part of Hypertable.
 * 
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; version 2 of
 * the License.
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

#include <fstream>
#include <iostream>
#include <string>

#include "Common/System.h"
#include "Common/Usage.h"

using namespace Hypertable;
using namespace std;

namespace {

  const char *usage[] = {
    "usage: merge_diff [options] <file-a> <file-b>",
    "",
    "  options:",
    "    --verify  Verify that each file is sorted along the way and report",
    "              an error if they are not.",
    "",
    "  This program performs a diff of two sorted files.  The files must be",
    "  sorted as if they had been created with the following commands:",
    "",
    "  $ LC_ALL=C sort <input-a> > <file-a>",
    "  $ LC_ALL=C sort <input-b> > <file-b>",
    "",
    "  The output is indeterminate if the files are not properly sorted.  To",
    "  verify that the files are properly sorted and the output is valid,",
    "  supply the --verify option",
    "",
    (const char *)0
  };
  
}


int main(int argc, char **argv) {
  const char *file_a=0, *file_b=0;
  bool verify=false;
  std::ifstream in_a;
  std::ifstream in_b;
  string line_a, line_b;
  int state = 0;
  int cmpval;
  bool a_cached = false;
  bool b_cached = false;

  System::initialize(argv[0]);

  for (int i=1; i<argc; i++) {
    if (!strcmp(argv[i], "--verify"))
      verify = true;
    else if (argv[i][0] == '-')
      Usage::dump_and_exit(usage);
    else if (file_a == 0)
      file_a = argv[i];
    else if (file_b == 0)
      file_b = argv[i];
    else
      Usage::dump_and_exit(usage);
  }

  if (file_b == 0)
    Usage::dump_and_exit(usage);

  in_a.open(file_a, ifstream::in);
  in_b.open(file_b, ifstream::in);

  while (in_a.good()) {

    if (!in_b.good()) {
      if (state == 2)
	cout << "---" << endl;
      if (a_cached) {
	cout << "< " << line_a << endl;
	a_cached = false;
      }
      while (in_a.good()) {
	if (getline(in_a, line_a))
	  cout << "< " << line_a << endl;
      }
      break;
    }

    if (!a_cached && !getline(in_a, line_a))
      continue;

    if (!b_cached && !getline(in_b, line_b)) {
      a_cached = true;
      continue;
    }

    if ((cmpval = strcmp(line_a.c_str(), line_b.c_str())) < 0) {
      a_cached = false;
      b_cached = true;
      if (state == 2)
	cout << "---" << endl;
      state = 1;
      cout << "< " << line_a << endl;
    }
    else if (cmpval > 0) {
      a_cached = true;
      b_cached = false;
      if (state == 1)
	cout << "---" << endl;
      state = 2;
      cout << "> " << line_b << endl;
    }
    else
      a_cached = b_cached = false;

  }

  if (in_b.good()) {
    if (state == 1)
      cout << "---" << endl;
    if (b_cached) {
      cout << "> " << line_b << endl;
      b_cached = false;
    }
    while (in_b.good()) {
      if (getline(in_b, line_b))
	cout << "> " << line_b << endl;
    }
  }

  in_a.close();
  in_b.close();
  

}
