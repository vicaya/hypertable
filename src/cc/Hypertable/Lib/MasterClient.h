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

#include "Hypertable/Lib/Types.h"


namespace Hypertable {

  class Comm;

  class MasterClient : public ReferenceCount {
  public:

    MasterClient(ConnectionManagerPtr &, Hyperspace::SessionPtr &,
                 time_t timeout, ApplicationQueuePtr &);
    ~MasterClient();

    /** Sets the client connection timeout
     *
     * @param timeout timeout value in seconds
     */
    void set_timeout(time_t timeout) { m_timeout = timeout; }

    int initiate_connection(DispatchHandlerPtr dhp);

    bool wait_for_connection(long max_wait_secs);

    int create_table(const char *tablename, const char *schemastr,
                     DispatchHandler *handler);
    int create_table(const char *tablename, const char *schemastr);

    int get_schema(const char *tablename, DispatchHandler *handler);
    int get_schema(const char *tablename, String &schema);

    int status();

    int register_server(const String &location, DispatchHandler *handler);
    int register_server(const String &location);

    int report_split(const TableIdentifier *table, const RangeSpec &range,
                     const char *log_dir, uint64_t soft_limit,
                     DispatchHandler *handler);
    int report_split(const TableIdentifier *table, const RangeSpec &range,
                     const char *log_dir, uint64_t soft_limit);

    int drop_table(const char *table_name, bool if_exists, DispatchHandler *);
    int drop_table(const char *table_name, bool if_exists);

    int shutdown();

    void reload_master();

    void set_verbose_flag(bool verbose) { m_verbose = verbose; }

  private:

    int send_message(CommBufPtr &cbp, DispatchHandler *handler);

    Mutex                  m_mutex;
    bool                   m_verbose;
    Comm                  *m_comm;
    ConnectionManagerPtr   m_conn_manager_ptr;
    Hyperspace::SessionPtr m_hyperspace_ptr;
    ApplicationQueuePtr    m_app_queue_ptr;
    time_t                 m_timeout;
    bool                   m_initiated;
    uint64_t               m_master_file_handle;
    Hyperspace::HandleCallbackPtr m_master_file_callback_ptr;
    struct sockaddr_in     m_master_addr;
    String                 m_master_addr_string;
    DispatchHandlerPtr     m_dispatcher_handler_ptr;
  };

  typedef intrusive_ptr<MasterClient> MasterClientPtr;


}

#endif // HYPERTABLE_MASTERCLIENT_H
