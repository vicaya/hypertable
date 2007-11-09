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

#include <boost/thread/mutex.hpp>

#include "AsyncComm/ApplicationQueue.h"
#include "AsyncComm/CommBuf.h"
#include "AsyncComm/ConnectionManager.h"
#include "AsyncComm/DispatchHandler.h"

#include "Common/ReferenceCount.h"

#include "Hyperspace/HandleCallback.h"
#include "Hyperspace/Session.h"

using namespace hypertable;

namespace hypertable {

  class Comm;

  class MasterClient : public ReferenceCount {
  public:

    MasterClient(ConnectionManagerPtr &connManagerPtr, Hyperspace::SessionPtr &hyperspacePtr, time_t timeout, ApplicationQueuePtr &appQueuePtr);
    ~MasterClient();

    int initiate_connection(DispatchHandlerPtr dispatchHandlerPtr);

    bool wait_for_connection(long maxWaitSecs);

    int create_table(const char *tableName, const char *schemaString, DispatchHandler *handler);
    int create_table(const char *tableName, const char *schemaString);

    int get_schema(const char *tableName, DispatchHandler *handler);
    int get_schema(const char *tableName, std::string &schema);

    int status();

    int register_server(std::string &location, DispatchHandler *handler);
    int register_server(std::string &location);

    /**
    int ReportSplit(const char *name, DispatchHandler *handler, uint32_t *msgIdp);
    int ReportSplit(const char *name, int32_t *fdp);
    **/

    int reload_master();

    void set_verbose_flag(bool verbose) { m_verbose = verbose; }

  private:

    int send_message(CommBufPtr &cbufPtr, DispatchHandler *handler);

    boost::mutex           m_mutex;
    bool                   m_verbose;
    Comm                  *m_comm;
    ConnectionManagerPtr   m_conn_manager_ptr;
    Hyperspace::SessionPtr m_hyperspace_ptr;
    ApplicationQueuePtr    m_app_queue_ptr;
    time_t                 m_timeout;
    bool                   m_initiated;
    uint64_t               m_master_file_handle;
    HandleCallbackPtr      m_master_file_callback_ptr;
    struct sockaddr_in     m_master_addr;
    std::string            m_master_addr_string;
    DispatchHandlerPtr     m_dispatcher_handler_ptr;
  };

  typedef boost::intrusive_ptr<MasterClient> MasterClientPtr;
  

}

#endif // HYPERTABLE_MASTERCLIENT_H
