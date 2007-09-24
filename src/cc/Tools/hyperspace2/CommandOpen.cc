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

#include <cstring>
#include <iostream>

#include "Common/Error.h"
#include "Common/Usage.h"

#include "CommandOpen.h"
#include "FileHandleCallback.h"
#include "Global.h"

using namespace hypertable;
using namespace Hyperspace;
using namespace std;

const char *CommandOpen::msUsage[] = {
  "open <fname> flags=[READ|WRITE|LOCK|CREATE|EXCL|TEMP] [event-mask=<mask>]",
  "  This command issues an OPEN request to Hyperspace.  The optional",
  "  parameter event-mask may take a value that is the combination of",
  "  the following strings:",
  "    ATTR_SET|ATTR_DEL|CHILD_NODE_ADDED|CHILD_NODE_REMOVED|LOCK_ACQUIRED|LOCK_RELEASED",
  (const char *)0
};

int CommandOpen::run() {
  std::string fname = "";
  int flags = 0;
  int eventMask = 0;
  char *str, *last;
  uint64_t handle;
  bool created;
  int error;

  for (size_t i=0; i<mArgs.size(); i++) {
    if (mArgs[i].first == "flags") {
      str = strtok_r((char *)mArgs[i].second.c_str(), " \t|", &last);
      while (str) {
	if (!strcmp(str, "READ"))
	  flags |= OPEN_FLAG_READ;
	else if (!strcmp(str, "WRITE"))
	  flags |= OPEN_FLAG_WRITE;
	else if (!strcmp(str, "LOCK"))
	  flags |= OPEN_FLAG_LOCK;
	else if (!strcmp(str, "CREATE"))
	  flags |= OPEN_FLAG_CREATE;
	else if (!strcmp(str, "EXCL"))
	  flags |= OPEN_FLAG_EXCL;
	else if (!strcmp(str, "TEMP"))
	  flags |= OPEN_FLAG_TEMP;
	str = strtok_r(0, " \t|", &last);
      }
    }
    else if (mArgs[i].first == "event-mask") {
      str = strtok_r((char *)mArgs[i].second.c_str(), " \t|", &last);
      while (str) {
	if (!strcmp(str, "ATTR_SET"))
	  eventMask |= EVENT_MASK_ATTR_SET;
	else if (!strcmp(str, "ATTR_DEL"))
	  eventMask |= EVENT_MASK_ATTR_DEL;
	else if (!strcmp(str, "CHILD_NODE_ADDED"))
	  eventMask |= EVENT_MASK_CHILD_NODE_ADDED;
	else if (!strcmp(str, "CHILD_NODE_REMOVED"))
	  eventMask |= EVENT_MASK_CHILD_NODE_REMOVED;
	else if (!strcmp(str, "LOCK_ACQUIRED"))
	  eventMask |= EVENT_MASK_LOCK_ACQUIRED;
	else if (!strcmp(str, "LOCK_RELEASED"))
	  eventMask |= EVENT_MASK_LOCK_RELEASED;
	str = strtok_r(0, " \t|", &last);
      }
    }
    else if (fname != "" || mArgs[i].second != "") {
      cerr << "Invalid arguments.  Type 'help' for usage." << endl;
      return -1;
    }
    else
      fname = mArgs[i].first;
  }

  if (flags == 0) {
    cerr << "Error: no flags supplied." << endl;
    return -1;
  }
  else if (fname == "") {
    cerr << "Error: no filename supplied." << endl;
    return -1;
  }

  //LOG_VA_INFO("open(%s, 0x%x, 0x%x)", fname.c_str(), flags, eventMask);

  HandleCallbackPtr callbackPtr = new FileHandleCallback(eventMask);

  if ((error = mSession->Open(fname, flags, callbackPtr, &handle, &created)) == Error::OK)
    Global::fileMap[fname] = handle;
  else {
    LOG_VA_ERROR("%s", Error::GetText(error));
  }
  return error;
}
