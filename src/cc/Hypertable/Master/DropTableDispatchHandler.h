/**
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_DROPTABLEDISPATCHHANDLER_H
#define HYPERTABLE_DROPTABLEDISPATCHHANDLER_H

#include "AsyncComm/Comm.h"
#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/Event.h"

#include "Common/SockAddrMap.h"

#include "Hypertable/Lib/RangeServerClient.h"

namespace Hypertable {

  /**
   * This class is a DispatchHandler class that is used for collecting
   * asynchronous drop table requests.
   */
  class DropTableDispatchHandler : public DispatchHandler {

  public:

    typedef struct {
      struct sockaddr_in addr;
      int error;
      std::string msg;
    } ErrorResultT;

    /**
     * Constructor.
     */
    DropTableDispatchHandler(std::string table_name, Comm *comm, time_t timeout);

    /**
     * Adds 
     */
    void add(struct sockaddr_in &addr);

    /**
     * Dispatch method.  This gets called by the AsyncComm layer
     * when an event occurs in response to a previously sent
     * request that was supplied with this dispatch handler.
     *
     * @param event_ptr shared pointer to event object
     */
    virtual void handle(EventPtr &event_ptr);

    bool wait_for_completion();

    void get_errors(vector<ErrorResultT> &errors);

  private:
    boost::mutex       m_mutex;
    boost::condition   m_cond;
    int                m_outstanding;
    RangeServerClient  m_client;
    std::string        m_table_name;
    vector<ErrorResultT> m_errors;
  };
}


#endif // HYPERTABLE_DROPTABLEDISPATCHHANDLER_H
