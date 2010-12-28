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
#include "Common/StringExt.h"

#include <boost/algorithm/string.hpp>

#include "boost/tokenizer.hpp"

namespace Hypertable {

using namespace std;
using namespace Hyperspace;


NameIdMapper::NameIdMapper(Hyperspace::SessionPtr &hyperspace, const String &toplevel_dir)
  : m_hyperspace(hyperspace), m_toplevel_dir(toplevel_dir) {
  uint64_t handle = 0;

  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle);

  /**
   * Prefix looks like this:  "/" <toplevel_dir> "namemap" "names"
   * This for loop adds the number of components in <toplevel_dir>
   */
  m_prefix_components = 3;
  for (const char *ptr=toplevel_dir.c_str(); *ptr; ptr++)
    if (*ptr == '/')
      m_prefix_components++;

  m_names_dir = toplevel_dir + "/namemap/names";
  m_ids_dir = toplevel_dir + "/namemap/ids";

  // Create the base namemap directory if it does not exist
  if (!m_hyperspace->exists(toplevel_dir + "/namemap")) {
    if (!m_hyperspace->exists(toplevel_dir))
      m_hyperspace->mkdirs(toplevel_dir);
    m_hyperspace->mkdir(toplevel_dir + "/namemap");
  }

  // Create "names" directory
  if (!m_hyperspace->exists(m_names_dir))
    m_hyperspace->mkdir(m_names_dir);

  // Create "ids" directory
  if (!m_hyperspace->exists(m_ids_dir)) {
    m_hyperspace->mkdir(m_ids_dir);
    handle = m_hyperspace->open(m_ids_dir,
                                OPEN_FLAG_READ|OPEN_FLAG_WRITE);
    m_hyperspace->attr_set(handle, "nid", "0", 1);
    m_hyperspace->close(handle);
    handle = 0;
  }
}

bool NameIdMapper::name_to_id(const String &name, String &id, bool *is_namespacep) {
  ScopedLock lock(m_mutex);
  return do_mapping(name, false, id, is_namespacep);
}

bool NameIdMapper::id_to_name(const String &id, String &name, bool *is_namespacep) {
  ScopedLock lock(m_mutex);
  return do_mapping(id, true, name, is_namespacep);
}

void NameIdMapper::add_entry(const String &names_parent, const String &names_entry, std::vector<uint64_t> &ids, bool is_namespace) {
  uint64_t handle_names_file = 0;
  uint64_t handle_ids_file = 0;
  uint64_t id = 0;
  String names_file = m_names_dir + names_parent + "/" + names_entry;


  if (is_namespace) {
    m_hyperspace->mkdir(names_file);
  }
  else {
    int oflags = OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE|OPEN_FLAG_EXCL;
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle_names_file);
    handle_names_file = m_hyperspace->open(names_file, oflags);
  }

  /**
   * Increment the "nid" attribute on the parent ID directory
   */
  String ids_file = m_ids_dir;
  for (size_t i=0; i<ids.size(); i++)
    ids_file += String("/") + ids[i];
  {
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle_ids_file);
    handle_ids_file = m_hyperspace->open(ids_file, OPEN_FLAG_READ|OPEN_FLAG_WRITE);
    id = m_hyperspace->attr_incr(handle_ids_file, "nid");
  }

  /**
   * Create the new ID directory and set it's "name" attribute
   */
  ids_file += String("/") + id;
  handle_ids_file = 0;
  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle_ids_file);
  if (is_namespace) {
    m_hyperspace->mkdir(ids_file);
    handle_ids_file = m_hyperspace->open(ids_file, OPEN_FLAG_READ|OPEN_FLAG_WRITE);
    m_hyperspace->attr_set(handle_ids_file, "nid", "0", 1);
  }
  else {
    int oflags = OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE|OPEN_FLAG_EXCL;
    handle_ids_file = m_hyperspace->open(ids_file, oflags);
  }

  m_hyperspace->attr_set(handle_ids_file, "name", names_entry.c_str(), names_entry.length());

  /**
   * Set the "id" attribute of the names file
   */
  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle_names_file);

  handle_names_file = m_hyperspace->open(names_file, OPEN_FLAG_READ|OPEN_FLAG_WRITE);
  char buf[16];
  sprintf(buf, "%llu", (Llu)id);
  m_hyperspace->attr_set(handle_names_file, "id", buf, strlen(buf));

  ids.push_back(id);

}

void NameIdMapper::add_mapping(const String &name, String &id, int flags) {
  ScopedLock lock(m_mutex);
  uint64_t handle = 0;
  typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
  boost::char_separator<char> sep("/");
  std::vector<String> name_components;
  std::vector<uint64_t> id_components;
  DynamicBuffer value_buf;

  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle);

  tokenizer tokens(name, sep);
  for (tokenizer::iterator tok_iter = tokens.begin();
       tok_iter != tokens.end(); ++tok_iter)
    name_components.push_back(*tok_iter);

  HT_ASSERT(!name_components.empty());

  String names_parent = "";
  String names_child = "";

  for (size_t i=0; i<name_components.size()-1; i++) {

    names_child += String("/") + name_components[i];

    try {
      String names_file = m_names_dir + names_child;
      handle = m_hyperspace->open(names_file, OPEN_FLAG_READ);
      m_hyperspace->attr_get(handle, "id", value_buf);
      m_hyperspace->close(handle);
      handle = 0;
      id_components.push_back( strtoll((const char *)value_buf.base, 0, 0) );
    }
    catch (Exception &e) {

      if (e.code() == Error::HYPERSPACE_FILE_EXISTS)
         HT_THROW(Error::NAMESPACE_EXISTS, (String)" namespace=" + name);

      if (e.code() != Error::HYPERSPACE_BAD_PATHNAME &&
          e.code() != Error::HYPERSPACE_FILE_NOT_FOUND)
        throw;


      if (!(flags & CREATE_INTERMEDIATE))
        HT_THROW(Error::NAMESPACE_DOES_NOT_EXIST, names_child);

      add_entry(names_parent, name_components[i], id_components, true);
    }

    names_parent = names_child;
  }

  try {
    add_entry(names_parent, name_components.back(),
        id_components, (bool)(flags & IS_NAMESPACE));
  }
  catch (Exception &e) {
    if (e.code() == Error::HYPERSPACE_FILE_EXISTS)
      HT_THROW(Error::NAMESPACE_EXISTS, (String)" namespace=" + name);
    throw;
  }

  id = (name[0] == '/') ? "/" : "";
  id = id + id_components[0];

  for (size_t i=1; i<id_components.size(); i++)
    id += String("/") + id_components[i];
}

void NameIdMapper::rename(const String &curr_name, const String &next_name) {
  ScopedLock lock(m_mutex);
  String id;
  int oflags = OPEN_FLAG_WRITE|OPEN_FLAG_EXCL|OPEN_FLAG_READ;
  uint64_t id_handle, name_handle;
  String old_name = curr_name;
  String new_name = next_name;
  String new_name_last_comp;
  size_t new_name_last_slash_pos;
  String id_last_component;
  size_t id_last_component_pos;

  id_handle = name_handle = 0;
  boost::trim_if(old_name, boost::is_any_of("/ "));
  boost::trim_if(new_name, boost::is_any_of("/ "));

  new_name_last_slash_pos = new_name.find_last_of('/');
  if (new_name_last_slash_pos != String::npos)
    new_name_last_comp = new_name.substr(new_name_last_slash_pos+1);
  else
    new_name_last_comp = new_name;

  if (do_mapping(old_name, false, id, 0)) {
    // Set the name attribute of the id file to be the last path component of new_name
    String id_file = m_ids_dir + "/" + id;
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &id_handle);
    id_handle = m_hyperspace->open(id_file, oflags);
    m_hyperspace->attr_set(id_handle, "name", new_name_last_comp.c_str(),
                           new_name_last_comp.length());

    // Create the name file and set its id attribute
    id_last_component_pos = id.find_last_of('/');
    if (id_last_component_pos != String::npos)
      id_last_component = id.substr(id_last_component_pos+1);
    else
      id_last_component = id;

    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &name_handle);
    name_handle = m_hyperspace->open(m_names_dir + "/" + new_name, oflags|OPEN_FLAG_CREATE);
    m_hyperspace->attr_set(name_handle, "id", id_last_component.c_str(),
                           id_last_component.length());

    // Delete the existing name file
    m_hyperspace->unlink(m_names_dir + "/" + old_name);
  }

}

void NameIdMapper::drop_mapping(const String &name) {
  ScopedLock lock(m_mutex);
  String id;
  String table_name = name;

  boost::trim_if(table_name, boost::is_any_of("/ "));

  if (do_mapping(name, false, id, 0))
    m_hyperspace->unlink(m_ids_dir + "/" + id);

  m_hyperspace->unlink(m_names_dir + "/" + table_name);
}

bool NameIdMapper::do_mapping(const String &input, bool id_in, String &output,
                              bool *is_namespacep) {
  vector <struct DirEntryAttr> listing;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
  uint64_t handle = 0;
  int num_path_components = 0;
  String hyperspace_file;
  String attr;
  output = (String)"";

  if (id_in) {
    hyperspace_file = m_ids_dir;
    attr = (String)"name";
  }
  else {
    hyperspace_file = m_names_dir;
    attr = (String)"id";
  }

  // deal with leading '/' in input
  if (input.find('/') != 0)
    hyperspace_file += "/";

  hyperspace_file += input;

  // count number of components in path (ignore  trailing '/')
  num_path_components = 1;
  for(size_t ii=0; ii< hyperspace_file.size()-1; ++ii) {
    if (hyperspace_file[ii] == '/')
      ++num_path_components;
  }

  try {
    handle = m_hyperspace->open(hyperspace_file, oflags);
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle);
    m_hyperspace->readpath_attr(handle, attr, listing);
  }
  catch (Exception &e) {
    if (e.code() == Error::HYPERSPACE_FILE_NOT_FOUND ||
        e.code() == Error::HYPERSPACE_BAD_PATHNAME) {
      HT_DEBUG_OUT << "Can't map " << input << HT_END;
      return false;
    }
  }

  if (listing.size() != (size_t) num_path_components || listing.size() == 0)
    return false;

  struct LtDirEntryAttr ascending;
  sort(listing.begin(), listing.end(), ascending);

  for (size_t ii=m_prefix_components; ii<listing.size(); ++ii) {
    String attr_val((const char*)listing[ii].attr.base);
    output += attr_val + "/";
  }

  // strip trailing slash
  if (!output.empty())
    output.resize(output.size()-1);

  bool is_dir = listing[listing.size()-1].is_dir;

  if (is_namespacep)
    *is_namespacep = is_dir;

  return true;

}

void NameIdMapper::id_to_sublisting(const String &id, bool include_sub_entries, vector<NamespaceListing> &listing) {
  vector <struct DirEntryAttr> dir_listing;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
  uint64_t handle = 0;
  String hyperspace_dir;
  String attr;

  hyperspace_dir = m_ids_dir;
  attr = (String)"name";

  hyperspace_dir += (String)"/" + id;

  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle);
  handle = m_hyperspace->open(hyperspace_dir, oflags);
  m_hyperspace->readdir_attr(handle, attr, include_sub_entries, dir_listing);

  get_namespace_listing(dir_listing, listing);
}

void NameIdMapper::get_namespace_listing(const std::vector<DirEntryAttr> &dir_listing, std::vector<NamespaceListing> &listing) {
  NamespaceListing entry;
  listing.clear();
  listing.reserve(dir_listing.size());
  foreach(const DirEntryAttr &dir_entry, dir_listing) {
    if (dir_entry.has_attr) {
      entry.name = (String)((const char*)dir_entry.attr.base);
      entry.is_namespace = dir_entry.is_dir;
      entry.id = dir_entry.name;
      listing.push_back(entry);
      if (!dir_entry.sub_entries.empty())
        get_namespace_listing(dir_entry.sub_entries, listing.back().sub_entries);
    }
  }

  struct LtNamespaceListingName ascending;
  sort(listing.begin(), listing.end(), ascending);
}
} // namespace Hypertable

