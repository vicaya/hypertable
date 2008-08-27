/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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
#include <cassert>
#include <cstring>

#include "Common/Error.h"

#include "Global.h"
#include "MetadataRoot.h"


using namespace Hypertable;
using namespace Hyperspace;

/**
 *
 */
MetadataRoot::MetadataRoot(SchemaPtr &schema_ptr) : m_next(0) {
  HandleCallbackPtr null_callback;
  typedef std::list<Schema::AccessGroup *> AgList;
  AgList *ag_list = schema_ptr->get_access_group_list();
  for (AgList::iterator ag_iter = ag_list->begin(); ag_iter != ag_list->end(); ag_iter++)
    m_agnames.push_back((*ag_iter)->name);

  try {
    m_handle = Global::hyperspace_ptr->open("/hypertable/root", OPEN_FLAG_READ, null_callback);
  }
  catch (Exception &e) {
    HT_ERRORF("Problem opening Hyperspace root file '/hypertable/root' - %s - %s", Error::get_text(e.code()), e.what());
    HT_ABORT;
  }

}



/**
 *
 */
MetadataRoot::~MetadataRoot() {
  try {
    Global::hyperspace_ptr->close(m_handle);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}



void MetadataRoot::reset_files_scan() {
  m_next = 0;
}



bool MetadataRoot::get_next_files(std::string &ag_name, std::string &files) {

  while (m_next < m_agnames.size()) {
    DynamicBuffer value(0);
    std::string attrname = (std::string)"files." + m_agnames[m_next];
    m_next++;

    try {
      Global::hyperspace_ptr->attr_get(m_handle, attrname.c_str(), value);
    }
    catch (Exception &e) {
      if (e.code() == Error::HYPERSPACE_ATTR_NOT_FOUND)
        continue;
      HT_ERRORF("Problem getting attribute '%s' on Hyperspace file '/hypertable/root' - %s",
		attrname.c_str(), Error::get_text(e.code()));
      return false;
    }
    files = (const char *)value.base;
    return true;
  }

  return false;
}



void MetadataRoot::write_files(std::string &ag_name, std::string &files) {
  std::string attrname = (std::string)"files." + ag_name;

  try {
    Global::hyperspace_ptr->attr_set(m_handle, attrname.c_str(), files.c_str(), files.length());
  }
  catch (Exception &e) {
    HT_THROW2(e.code(), e, (std::string)"Problem creating attribute '" + attrname + "' on Hyperspace file '/hypertable/root'");
  }

}
