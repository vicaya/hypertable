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

#ifndef HYPERTABLE_MASTERCLIENT_H
#define HYPERTABLE_MASTERCLIENT_H

#include "Common/Properties.h"
#include "AsyncComm/Comm.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/DispatchHandler.h"

using namespace hypertable;

namespace hypertable {

  class MasterClient {
  public:

    MasterClient(Comm *comm, Properties *props);
    ~MasterClient();

    bool WaitForConnection(long maxWaitSecs) {
      return mConnectionManager.WaitForConnection(maxWaitSecs);
    }

    int CreateTable(const char *tableName, const char *schemaString, DispatchHandler *handler, uint32_t *msgIdp);
    int CreateTable(const char *tableName, const char *schemaString, int32_t *fdp);

    int GetSchema(const char *tableName, DispatchHandler *handler, uint32_t *msgIdp);
    int GetSchema(const char *tableName, std::string &schema);

    /**
    int ReportSplit(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int ReportSplit(const char *name, int32_t *fdp);
    **/

  private:
    Comm              *mComm;
    ConnectionManager  mConnectionManager;
  };

}

#endif // HYPERTABLE_MASTERCLIENT_H
