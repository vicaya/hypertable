/** -*- c++ -*-
 * Copyright (C) 2010 Sanjit Jhala(Hypertable, Inc.)
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

#ifndef HYPERTABLE_DISPATCHHANDLERGETSTATISTICS_H
#define HYPERTABLE_DISPATCHHANDLERGETSTATISTICS_H

#include <vector>

#include "AsyncComm/Comm.h"
#include "AsyncComm/CommAddress.h"
#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/Event.h"

#include "Hypertable/Lib/RangeServerClient.h"

#include "RangeServerStatistics.h"

namespace Hypertable {

  using namespace std;
  /**
   * This class is a DispatchHandler class that is used for collecting
   * asynchronous update schema requests.
   */
  class DispatchHandlerGetStatistics : public DispatchHandler {

  public:

    typedef CommAddressMap<RangeServerStatistics *> GetStatisticsResponseMap;

    /**
     * Constructor.
     */
    DispatchHandlerGetStatistics(Comm *comm, time_t timeout);

    /**
     * Adds
     */
    void add(std::vector<RangeServerStatistics> &stats);

    /**
     * Dispatch method.
     *
     * @param event reference to event pointer
     */
    virtual void handle(EventPtr &event);

    /**
     *
     */
    void wait_for_completion();

  private:
    Mutex              m_mutex;
    boost::condition   m_cond;
    int                m_outstanding;
    RangeServerClient  m_client;
    GetStatisticsResponseMap m_response;
  };
}


#endif // HYPERTABLE_DISPATCHHANDLERGETSTATISTICS_H
