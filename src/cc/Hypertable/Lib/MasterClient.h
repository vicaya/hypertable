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
#include "Common/Timer.h"

#include "Hyperspace/HandleCallback.h"
#include "Hyperspace/Session.h"

#include "Hypertable/Lib/Types.h"


namespace Hypertable {

  class Comm;
  class MasterClient;
  class MasterClientHyperspaceSessionCallback: public Hyperspace::SessionCallback {
  public:
    MasterClientHyperspaceSessionCallback() {}
    ~MasterClientHyperspaceSessionCallback() {}
    void safe() {}
    void expired() {}
    void jeopardy() {}
    void disconnected();
    void reconnected();

  private:
    friend class MasterClient;
    MasterClient *m_masterclient;
  };

  class MasterClient : public ReferenceCount {
  public:

    MasterClient(ConnectionManagerPtr &, Hyperspace::SessionPtr &,
                 uint32_t timeout_ms, ApplicationQueuePtr &);
    ~MasterClient();

    void initiate_connection(DispatchHandlerPtr dhp);

    bool wait_for_connection(uint32_t max_wait_ms);
    bool wait_for_connection(Timer &timer);

    void create_table(const char *tablename, const char *schemastr,
                      DispatchHandler *handler, Timer *timer = 0);
    void create_table(const char *tablename, const char *schemastr,
                      Timer *timer = 0);
    void alter_table(const char *tablename, const char *schemastr,
                         DispatchHandler *handler, Timer *timer = 0);
    void alter_table(const char *tablename, const char *schemastr,
                     Timer *timer = 0);
    void get_schema(const char *tablename, DispatchHandler *handler,
                    Timer *timer = 0);
    void get_schema(const char *tablename, std::string &schema, Timer *timer=0);

    void status(Timer *timer=0);

    void register_server(std::string &location, DispatchHandler *handler,
                         Timer *timer = 0);
    void register_server(std::string &location, Timer *timer=0);

    void report_split(TableIdentifier *table, RangeSpec &range,
                      const char *log_dir, uint64_t soft_limit,
                      DispatchHandler *handler, Timer *timer = 0);
    void report_split(TableIdentifier *table, RangeSpec &range,
                      const char *log_dir, uint64_t soft_limit, Timer *timer=0);

    void drop_table(const char *table_name, bool if_exists,
                    DispatchHandler *handler, Timer *timer=0);
    void drop_table(const char *table_name, bool if_exists, Timer *timer=0);

    void close(Timer *timer=0);

    void shutdown(Timer *timer=0);

    void reload_master();

    void set_verbose_flag(bool verbose) { m_verbose = verbose; }

  private:
    friend class MasterClientHyperspaceSessionCallback;

    void hyperspace_disconnected();
    void hyperspace_reconnected();
    void send_message(CommBufPtr &cbp, DispatchHandler *handler, Timer *timer);
    void initialize_hyperspace();

    Mutex                  m_mutex;
    bool                   m_verbose;
    Comm                  *m_comm;
    ConnectionManagerPtr   m_conn_manager_ptr;
    Hyperspace::SessionPtr m_hyperspace;
    ApplicationQueuePtr    m_app_queue_ptr;
    uint64_t               m_master_file_handle;
    Hyperspace::HandleCallbackPtr m_master_file_callback_ptr;
    InetAddr               m_master_addr;
    String                 m_master_addr_string;
    DispatchHandlerPtr     m_dispatcher_handler_ptr;
    bool                   m_hyperspace_init;
    bool                   m_hyperspace_connected;
    Mutex                  m_hyperspace_mutex;
    uint32_t               m_timeout_ms;
    MasterClientHyperspaceSessionCallback m_hyperspace_session_callback;
  };

  typedef intrusive_ptr<MasterClient> MasterClientPtr;


}

#endif // HYPERTABLE_MASTERCLIENT_H
