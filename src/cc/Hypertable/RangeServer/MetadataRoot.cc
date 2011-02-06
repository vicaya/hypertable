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
#include <cstdlib>
#include <cstring>

#include "Common/Error.h"

#include "Global.h"
#include "MetadataRoot.h"

using namespace Hypertable;
using namespace Hyperspace;


MetadataRoot::MetadataRoot(SchemaPtr &schema) : m_next(0) {

  foreach(const Schema::AccessGroup *ag, schema->get_access_groups())
    m_agnames.push_back(ag->name);

  try {
    m_handle = Global::hyperspace->open(Global::toplevel_dir + "/root",
                                        OPEN_FLAG_READ);
  }
  catch (Exception &e) {
    HT_ERRORF("Problem opening Hyperspace root file '%s/root' - %s - "
              "%s", Global::toplevel_dir.c_str(), Error::get_text(e.code()), e.what());
    HT_ABORT;
  }

}


MetadataRoot::~MetadataRoot() {
  try {
    Global::hyperspace->close(m_handle);
  }
  catch (Exception &e) {
    HT_ERROR_OUT << e << HT_END;
  }
}


void MetadataRoot::reset_files_scan() {
  m_next = 0;
}


bool MetadataRoot::get_next_files(String &ag_name, String &files, uint32_t *nextcsidp) {

  while (m_next < m_agnames.size()) {
    DynamicBuffer value(0);
    String files_attrname = (String)"files." + m_agnames[m_next];
    String nextcsid_attrname = (String)"nextcsid." + m_agnames[m_next];
    ag_name = m_agnames[m_next];
    m_next++;

    *nextcsidp = 0;

    // Read nextcsid
    try {
      Global::hyperspace->attr_get(m_handle, nextcsid_attrname.c_str(), value);
      *nextcsidp = atoi((const char *)value.base);
    }
    catch (Exception &e) {
    }

    try {
      Global::hyperspace->attr_get(m_handle, files_attrname.c_str(), value);
    }
    catch (Exception &e) {
      if (e.code() == Error::HYPERSPACE_ATTR_NOT_FOUND)
        continue;
      HT_ERRORF("Problem getting attribute '%s' on Hyperspace file "
                "'%s/root' - %s", Global::toplevel_dir.c_str(), files_attrname.c_str(),
                Error::get_text(e.code()));
      return false;
    }
    files = (const char *)value.base;
    return true;
  }

  return false;
}


void MetadataRoot::write_files(const String &ag_name, const String &files) {
  String attrname = (String)"files." + ag_name;

  // Write "files"
  try {
    Global::hyperspace->attr_set(m_handle, attrname.c_str(), files.c_str(),
                                 files.length());
  }
  catch (Exception &e) {
    HT_THROW2(e.code(), e, (String)"Problem creating attribute '" + attrname
              + "' on Hyperspace file '/hypertable/root'");
  }

}

void MetadataRoot::write_files(const String &ag_name, const String &files, uint32_t nextcsid) {
  String attrname = (String)"nextcsid." + ag_name;
  char buf[32];

  sprintf(buf, "%u", (unsigned)nextcsid);

  write_files(ag_name, files);

  // Write "nextcsid"
  try {
    Global::hyperspace->attr_set(m_handle, attrname.c_str(), buf, strlen(buf));
  }
  catch (Exception &e) {
    HT_THROW2(e.code(), e, (String)"Problem creating attribute '" + attrname
              + "' on Hyperspace file '/hypertable/root'");
  }

}
