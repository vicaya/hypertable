/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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
#include "Common/Logger.h"

#include <cstdlib>
#include <vector>

#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/predicate.hpp>

#include "DiscreteRandomGeneratorFactory.h"
#include "DiscreteRandomGeneratorUniform.h"
#include "DiscreteRandomGeneratorZipf.h"

using namespace Hypertable;
using namespace std;
using namespace boost;


DiscreteRandomGenerator *
DiscreteRandomGeneratorFactory::create(const String &spec) {
  vector<String> args;
  String name;

  split(args, spec, is_any_of("\t "));

  if (!args.empty()) {
    name = args.front();
    args.erase(args.begin());
  }

  if (name == "uniform")
    return new DiscreteRandomGeneratorUniform();
  else if (name == "zipf") {
    if (args.empty())
      return new DiscreteRandomGeneratorZipf();
    if (starts_with(args[0], "--s=")) {
      String s_str = args[0].substr(4);
      double s_val;
      s_val = strtod(s_str.c_str(), 0);
      return new DiscreteRandomGeneratorZipf(s_val);
    }
  }

  HT_FATALF("Unrecognized distribution (%s)", name.c_str());

  return 0;
}
