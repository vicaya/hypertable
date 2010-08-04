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
  HandleCallbackPtr null_handle_callback;
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
                                OPEN_FLAG_READ|OPEN_FLAG_WRITE,
                                null_handle_callback);
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
  HandleCallbackPtr null_handle_callback;
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
    handle_names_file = m_hyperspace->open(names_file, oflags, null_handle_callback);
  }

  /**
   * Increment the "nid" attribute on the parent ID directory
   */
  String ids_file = m_ids_dir;
  for (size_t i=0; i<ids.size(); i++)
    ids_file += String("/") + ids[i];
  {
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle_ids_file);
    handle_ids_file = m_hyperspace->open(ids_file, OPEN_FLAG_READ|OPEN_FLAG_WRITE,
        null_handle_callback);
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
    handle_ids_file = m_hyperspace->open(ids_file, OPEN_FLAG_READ|OPEN_FLAG_WRITE,
        null_handle_callback);
    m_hyperspace->attr_set(handle_ids_file, "nid", "0", 1);
  }
  else {
    int oflags = OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE|OPEN_FLAG_EXCL;
    handle_ids_file = m_hyperspace->open(ids_file, oflags, null_handle_callback);
  }

  m_hyperspace->attr_set(handle_ids_file, "name", names_entry.c_str(), names_entry.length());

  /**
   * Set the "id" attribute of the names file
   */
  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle_ptr, m_hyperspace, &handle_names_file);

  handle_names_file = m_hyperspace->open(names_file, OPEN_FLAG_READ|OPEN_FLAG_WRITE,
      null_handle_callback);
  char buf[16];
  sprintf(buf, "%llu", (Llu)id);
  m_hyperspace->attr_set(handle_names_file, "id", buf, strlen(buf));

  ids.push_back(id);

}

void NameIdMapper::add_mapping(const String &name, String &id, int flags) {
  ScopedLock lock(m_mutex);
  uint64_t handle = 0;
  HandleCallbackPtr null_handle_callback;
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
      handle = m_hyperspace->open(names_file, OPEN_FLAG_READ, null_handle_callback);
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
  HandleCallbackPtr null_handle_callback;
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
    handle = m_hyperspace->open(hyperspace_file, oflags, null_handle_callback);
    HT_ON_SCOPE_EXIT(&Hyperspace::close_handle, m_hyperspace, handle);
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

void NameIdMapper::id_to_sublisting(const String &id, vector<NamespaceListing> &listing) {
  vector <struct DirEntryAttr> dir_listing;
  uint32_t oflags = OPEN_FLAG_READ | OPEN_FLAG_WRITE | OPEN_FLAG_LOCK;
  uint64_t handle = 0;
  HandleCallbackPtr null_handle_callback;
  String hyperspace_dir;
  String attr;
  NamespaceListing entry;

  hyperspace_dir = m_ids_dir;
  attr = (String)"name";

  hyperspace_dir += (String)"/" + id;

  HT_ON_SCOPE_EXIT(&Hyperspace::close_handle, m_hyperspace, handle);
  handle = m_hyperspace->open(hyperspace_dir, oflags, null_handle_callback);
  m_hyperspace->readdir_attr(handle, attr, dir_listing);

  listing.clear();
  for (size_t ii=0; ii<dir_listing.size(); ++ii) {
    if (dir_listing[ii].has_attr) {
      entry.name = (String)((const char*)dir_listing[ii].attr.base);
      entry.is_namespace = dir_listing[ii].is_dir;
      entry.id = dir_listing[ii].name;
      listing.push_back(entry);
    }
  }

  struct LtNamespaceListingName ascending;
  sort(listing.begin(), listing.end(), ascending);

}
} // namespace Hypertable

