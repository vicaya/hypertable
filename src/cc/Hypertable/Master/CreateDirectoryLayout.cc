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

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Properties.h"
#include "AsyncComm/Comm.h"

#include "Hyperspace/Session.h"

#include "CreateDirectoryLayout.h"

extern "C" {
#include <stdint.h>
}

using namespace hypertable;


namespace {

  bool CreateDirectory(Hyperspace::Session *hyperspace, std::string dir) {
    int error;
    bool exists;

    /**
     * Check for existence
     */
    if ((error = hyperspace->Exists(dir, &exists)) != Error::OK) {
      LOG_VA_ERROR("Problem checking for existence of directory '%s' - %s", dir.c_str(), Error::GetText(error));
      return false;
    }

    if (exists)
      return true;

    if ((error = hyperspace->Mkdir(dir)) != Error::OK) {
      LOG_VA_ERROR("Problem creating directory '%s' - %s", dir.c_str(), Error::GetText(error));
      return false;
    }

    return true;
  }

}


bool hypertable::CreateDirectoryLayout(ConnectionManager *connManager, PropertiesPtr &propsPtr) {
  int error;
  uint64_t handle;
  uint32_t tableId = 0;
  HandleCallbackPtr nullHandleCallback;
  Hyperspace::Session *hyperspace;

  hyperspace = new Hyperspace::Session(connManager->GetComm(), propsPtr, 0);

  if (!hyperspace->WaitForConnection(30)) {
    LOG_ERROR("Unable to connect to hyperspace, exiting...");
    exit(1);
  }

  if (!CreateDirectory(hyperspace, "/hypertable"))
    return false;

  if (!CreateDirectory(hyperspace, "/hypertable/servers"))
    return false;

  if (!CreateDirectory(hyperspace, "/hypertable/tables"))
    return false;

  /**
   * Create /hypertable/master
   */
  if ((error = hyperspace->Open("/hypertable/master", OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE, nullHandleCallback, &handle)) != Error::OK) {
    LOG_VA_ERROR("Unable to open Hyperspace file '/hypertable/master' (%s)", Error::GetText(error));
    return false;
  }
  tableId = 0;
  if ((error = hyperspace->AttrSet(handle, "last_table_id", &tableId, sizeof(int32_t))) != Error::OK) {
    LOG_VA_ERROR("Problem setting attribute 'last_table_id' of file /hypertable/master - %s", Error::GetText(error));
    hyperspace->Close(handle);
    return false;
  }
  hyperspace->Close(handle);

  /**
   *  Create /hypertable/root
   */
  if ((error = hyperspace->Open("/hypertable/root", OPEN_FLAG_READ|OPEN_FLAG_WRITE|OPEN_FLAG_CREATE, nullHandleCallback, &handle)) != Error::OK) {
    LOG_VA_ERROR("Unable to open Hyperspace file '/hypertable/root' (%s)", Error::GetText(error));
    return false;
  }
  hyperspace->Close(handle);

  return true;
}
