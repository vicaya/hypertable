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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

#include <iostream>
#include <string>

#include "Global.h"
#include "Util.h"

void Hyperspace::Util::normalize_pathname(std::string name, std::string &normalName) {
  normalName = "";
  if (name[0] != '/')
    normalName += "/";

  if (name.find('/', name.length()-1) == std::string::npos)
    normalName += name;
  else
    normalName += name.substr(0, name.length()-1);
}

bool Hyperspace::Util::get_handle(std::string name, uint64_t *handlep) {
  std::string normalName;

  normalize_pathname(name, normalName);

  Global::FileMapT::iterator iter = Global::fileMap.find(normalName);
  if (iter == Global::fileMap.end()) {
    std::cerr << "Unable to find '" << normalName << "' in open file map" << std::endl;
    return false;
  }
  *handlep = (*iter).second;
  return true;
}
