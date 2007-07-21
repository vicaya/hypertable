/**
 * Copyright 2007 Doug Judd (Zvents, Inc.)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0 
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
