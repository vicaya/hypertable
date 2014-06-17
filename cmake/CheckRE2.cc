/** -*- C++ -*-
 * Copyright (C) 2010  Sanjit Jhala (sanjit@hypertable.org)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#include <stdio.h>
#include <string.h>
#include <re2/re2.h>


int main() {
  using namespace std;

  string hello("hello");
  string re1("h.*o");
  string re2("h.*0");
  if (!RE2::FullMatch(hello.c_str(), re1.c_str())) {
    fprintf (stderr, "'%s' should match pattern '%s'", hello.c_str(), re1.c_str());
    return 1;
  }
  if (RE2::FullMatch(hello.c_str(), re2.c_str())) {
    fprintf (stderr, "'%s' should not match pattern '%s'", hello.c_str(), re2.c_str());
    return 1;
  }
  printf("0.0.0\n");
  return 0;
}
