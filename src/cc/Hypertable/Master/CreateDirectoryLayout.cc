/**
 * Copyright (C) 2007 Doug Judd (Zvents, Inc.)
 * 
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or any later version.
 * 
 * This program is distributed in the hope that it will be useful,
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
#include "Hyperspace/HyperspaceClient.h"

#include "CreateDirectoryLayout.h"

extern "C" {
#include <stdint.h>
}

using namespace hypertable;

bool hypertable::CreateDirectoryLayout(Comm *comm, Properties *props) {
  HyperspaceClient *hyperspace;
  int error;

  hyperspace = new HyperspaceClient(comm, props);

  if (!hyperspace->WaitForConnection()) {
    LOG_ERROR("Unable to connect to hyperspace, exiting...");
    exit(1);
  }

  /**
   * Create /hypertable/servers directory
   */
  error = hyperspace->Exists("/hypertable/servers");
  if (error == Error::HYPERTABLEFS_FILE_NOT_FOUND) {
    if ((error = hyperspace->Mkdirs("/hypertable/servers")) != Error::OK) {
      LOG_VA_ERROR("Problem creating hyperspace directory '/hypertable/servers' - %s", Error::GetText(error));
      return false;
    }
  }
  else if (error != Error::OK)
    return false;


  /**
   * Create /hypertable/tables directory
   */
  error = hyperspace->Exists("/hypertable/tables");
  if (error == Error::HYPERTABLEFS_FILE_NOT_FOUND) {
    if (hyperspace->Mkdirs("/hypertable/tables") != Error::OK)
      return false;
  }
  else if (error != Error::OK)
    return false;


  /**
   * Create /hypertable/master directory
   */
  error = hyperspace->Exists("/hypertable/master");
  if (error == Error::HYPERTABLEFS_FILE_NOT_FOUND) {
    if (hyperspace->Mkdirs("/hypertable/master") != Error::OK)
      return false;
  }
  else if (error != Error::OK)
    return false;

  /**
   * Create /hypertable/meta directory
   */
  error = hyperspace->Exists("/hypertable/meta");
  if (error == Error::HYPERTABLEFS_FILE_NOT_FOUND) {
    if (hyperspace->Mkdirs("/hypertable/meta") != Error::OK)
      return false;
  }
  else if (error != Error::OK)
    return false;

  /**
   * 
   */
  if ((error = hyperspace->AttrSet("/hypertable/meta", "last_table_id", "0")) != Error::OK)
    return false;

  return true;
}
