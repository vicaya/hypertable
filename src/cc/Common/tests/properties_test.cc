/** -*- C++ -*-
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
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

#include "Common/Compat.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include <iostream>

using namespace Hypertable;
using namespace Property;

namespace {

void basic_test(const PropertiesDesc &desc) {
  static char *args[] = { "test", "--str", "foo", "--strs", "s1",
      "--i16", "64k", "--i32", "20M", "--i64", "8g", "--strs", "s2",
      "--i64s", "16G", "--i64s", "32G" };
  Properties props;
  props.parse_args(sizeof(args)/sizeof(char *), args, desc);
  props.print(std::cout);
}

}

int main(int argc, char *argv[]) {
  try {
    PropertiesDesc desc;
    desc.add_options()
      ("help,h", "help")
      ("boo", boo()->zero_tokens()->default_value(true), "a boolean arg")
      ("str", str(), "a string arg")
      ("strs", strs(), "a list of strings")
      ("i16", i16(), "a 16-bit integer arg")
      ("i32", i32(), "a 32-bit integer arg")
      ("i64", i64(), "a 64-bit integer arg")
      ("i64s", i64s(), "a list of 64-bit integers")
      ;

    Properties props;
    props.parse_args(argc, argv, desc);

    if (props.has("help"))
      std::cout << desc << std::endl;

    props.print(std::cout, true);
    HT_TRY("basic test", basic_test(desc));
  }
  catch (Exception &e) {
    std::cout << e << std::endl;
    return 1;
  }
  return 0;
}
