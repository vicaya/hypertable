/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#include "Common/Compat.h"
#include <iostream>
#include <string>

#include "Global.h"
#include "Util.h"

using namespace Hypertable;

void Hyperspace::Util::normalize_pathname(std::string name, std::string &normal_name) {
  normal_name = "";
  if (name[0] != '/')
    normal_name += "/";

  if (name.find('/', name.length()-1) == std::string::npos)
    normal_name += name;
  else
    normal_name += name.substr(0, name.length()-1);
}

uint64_t Hyperspace::Util::get_handle(std::string name) {
  std::string normal_name;

  normalize_pathname(name, normal_name);

  Global::FileMap::iterator iter = Global::file_map.find(normal_name);
  if (iter == Global::file_map.end())
    HT_THROWF(Error::PARSE_ERROR, "Unable to find '%s' in open file map", normal_name.c_str());

  return (*iter).second;
}
