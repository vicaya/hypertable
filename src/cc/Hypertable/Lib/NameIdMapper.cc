/** -*- c++ -*-
 * Copyright (C) 2010 Sanjit Jhala (Hypertable, Inc.)
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

#include "NameIdMapper.h"
#include "Common/ScopeGuard.h"
#include "Common/Error.h"
#include "Common/Logger.h"

using namespace std;
using namespace Hyperspace;


void close_hyperspace_file(Hyperspace::SessionPtr hyperspace, uint64_t handle) {
  if (handle)
    hyperspace->close(handle);
}


namespace Hypertable {
const String NameIdMapper::HS_DIR = "/hypertable";
const String NameIdMapper::HS_NAMEMAP_DIR = HS_DIR + "/namemap";
const String NameIdMapper::HS_NAMEMAP_NAMES_DIR = HS_NAMEMAP_DIR + "/names";
const String NameIdMapper::HS_NAMEMAP_IDS_DIR = HS_NAMEMAP_DIR + "/ids";
const int NameIdMapper::HS_NAMEMAP_COMPONENTS = 4;

NameIdMapper::NameIdMapper(Hyperspace::SessionPtr &hyperspace) : m_hyperspace(hyperspace) {}

bool NameIdMapper::name_to_id(const String &name, String &id) {
  return do_mapping(name, false, id);
}

bool NameIdMapper::id_to_name(const String &id, String &name) {
  return do_mapping(id, true, name);
}

bool NameIdMapper::do_mapping(const String &input, bool id_in, String &output) {
  vector <struct DirEntryAttr> listing;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
  uint64_t handle = 0;
  int num_path_components = 0;
  HandleCallbackPtr null_handle_callback;
  String hyperspace_file;
  String attr;
  output = (String)"";

  if (id_in) {
    hyperspace_file = HS_NAMEMAP_IDS_DIR;
    attr = (String)"name";
  }
  else {
    hyperspace_file = HS_NAMEMAP_NAMES_DIR;
    attr = (String)"id";
  }

  // deal with leading '/' in input
  if (input.find('/') == 0)
    // input has a leading '/' , so shd the output
    output += "/";
  else
    hyperspace_file += "/";

  hyperspace_file += input;

  // count number of components in path (ignore  trailing '/')
  num_path_components = 1;
  for(size_t ii=0; ii< hyperspace_file.size()-1; ++ii) {
    if (hyperspace_file[ii] == '/')
      ++num_path_components;
  }

  try {
    handle = m_hyperspace->open(hyperspace_file, oflags, null_handle_callback);
    HT_ON_SCOPE_EXIT(&close_hyperspace_file, m_hyperspace, handle);
    m_hyperspace->readpath_attr(handle, attr, listing);
  }
  catch (Exception &e) {
    if (e.code() == Error::HYPERSPACE_FILE_NOT_FOUND ||
        e.code() == Error::HYPERSPACE_BAD_PATHNAME) {
      HT_ERROR_OUT << "Can't map " << input << HT_END;
      return false;
    }
  }

  if (listing.size() != (size_t) num_path_components)
    return false;

  struct LtDirEntryAttr ascending;
  sort(listing.begin(), listing.end(), ascending);

  for (size_t ii=HS_NAMEMAP_COMPONENTS; ii<listing.size(); ++ii) {
    String attr_val((const char*)listing[ii].attr.base);
    output += attr_val + "/";
  }

  // if input had no trailing slash then remove trailing slash from output
  if (hyperspace_file[hyperspace_file.size()-1] != '/')
    output.resize(output.size()-1);

  return true;

}

} // namespace Hypertable

