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
#include <cstring>
#include <iostream>

#include "Common/Error.h"
#include "Common/Usage.h"

#include "CommandCreate.h"
#include "FileHandleCallback.h"
#include "Global.h"
#include "Util.h"

using namespace Hypertable;
using namespace Hyperspace;
using namespace std;

const char *CommandCreate::ms_usage[] = {
  "create <fname> flags=[READ|WRITE|LOCK|TEMP|LOCK_SHARED|LOCK_EXCLUSIVE] [OPTIONS]",
  "OPTIONS:",
  "  attr:<name>=<value>  This can be used to specify extended attributes that should",
  "                       get created atomically when the file is created",
  "  event-mask=<mask>  This indicates which events should be delivered on this handle.",
  "                     The <mask> value can take any combination of the following:",
  "                     ATTR_SET|ATTR_DEL|LOCK_ACQUIRED|LOCK_RELEASED",
  "  This command issues an CREATE request to Hyperspace.",
  (const char *)0
};

void CommandCreate::run() {
  std::string fname = "";
  int flags = 0;
  int event_mask = 0;
  char *str, *last;
  uint64_t handle;
  std::vector<Attribute> init_attrs;
  Attribute attr;

  for (size_t i=0; i<m_args.size(); i++) {
    if (m_args[i].first == "flags") {
      str = strtok_r((char *)m_args[i].second.c_str(), " \t|", &last);
      while (str) {
        if (!strcmp(str, "READ"))
          flags |= OPEN_FLAG_READ;
        else if (!strcmp(str, "WRITE"))
          flags |= OPEN_FLAG_WRITE;
        else if (!strcmp(str, "LOCK"))
          flags |= OPEN_FLAG_LOCK;
        else if (!strcmp(str, "TEMP"))
          flags |= OPEN_FLAG_TEMP;
        else if (!strcmp(str, "LOCK_SHARED"))
          flags |= OPEN_FLAG_LOCK_SHARED;
        else if (!strcmp(str, "LOCK_EXCLUSIVE"))
          flags |= OPEN_FLAG_LOCK_EXCLUSIVE;
        str = strtok_r(0, " \t|", &last);
      }
    }
    else if (m_args[i].first == "event-mask") {
      str = strtok_r((char *)m_args[i].second.c_str(), " \t|", &last);
      while (str) {
        if (!strcmp(str, "ATTR_SET"))
          event_mask |= EVENT_MASK_ATTR_SET;
        else if (!strcmp(str, "ATTR_DEL"))
          event_mask |= EVENT_MASK_ATTR_DEL;
        else if (!strcmp(str, "LOCK_ACQUIRED"))
          event_mask |= EVENT_MASK_LOCK_ACQUIRED;
        else if (!strcmp(str, "LOCK_RELEASED"))
          event_mask |= EVENT_MASK_LOCK_RELEASED;
        str = strtok_r(0, " \t|", &last);
      }
    }
    else if (m_args[i].first.find("attr:", 0) == 0) {
      attr.name = m_args[i].first.c_str() + 5;
      attr.value = m_args[i].second.c_str();
      attr.value_len = strlen((const char *)attr.value);
      init_attrs.push_back(attr);
    }
    else if (fname != "" || m_args[i].second != "")
      HT_THROW(Error::PARSE_ERROR, "Invalid arguments.  Type 'help' for usage.");
    else
      fname = m_args[i].first;
  }

  if (flags == 0)
    HT_THROW(Error::PARSE_ERROR, "Error: no flags supplied.");
  else if (fname == "")
    HT_THROW(Error::PARSE_ERROR, "Error: no filename supplied.");

  //HT_INFOF("create(%s, 0x%x, 0x%x)", fname.c_str(), flags, event_mask);

  HandleCallbackPtr callback = new FileHandleCallback(event_mask);

  handle = m_session->create(fname, flags, callback, init_attrs);

  std::string normal_name;
  Util::normalize_pathname(fname, normal_name);
  Global::file_map[normal_name] = handle;

}
