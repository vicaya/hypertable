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

#include "Common/Compat.h"
#include "AsyncComm/Protocol.h"

#include "Common/Error.h"
#include "Common/Logger.h"
#include "Common/Time.h"

#include "DispatchHandlerGetStatistics.h"

using namespace Hypertable;


DispatchHandlerGetStatistics::DispatchHandlerGetStatistics(Comm *comm, time_t timeout)
  : m_outstanding(0), m_client(comm, timeout) {
}


void DispatchHandlerGetStatistics::add(std::vector<RangeServerStatistics> &stats) {
  ScopedLock lock(m_mutex);
  CommAddress addr;
  int64_t now = get_ts64();

  for (size_t i=0; i<stats.size(); i++) {
    m_response[stats[i].addr] = &stats[i];
    stats[i].fetch_error = Error::NO_RESPONSE;
    stats[i].fetch_timestamp = now;
    stats[i].stats = new StatsRangeServer();
    try {
      addr.set_proxy(stats[i].location);
      m_client.get_statistics(addr, this);
      m_outstanding++;
    }
    catch (Exception &e) {
      stats[i].fetch_error = e.code();
      stats[i].fetch_error_msg = "Send error";
    }
  }

}


void DispatchHandlerGetStatistics::handle(EventPtr &event) {
  ScopedLock lock(m_mutex);
  RangeServerStatistics *statsp = 0;
  int error;
  int64_t now = get_ts64();
  CommAddress addr(event->addr);
  
  GetStatisticsResponseMap::iterator iter = m_response.find(addr);

  if (iter == m_response.end()) {
    HT_ERROR_OUT << "Received 'get_statistics' response from unexpected connection " 
                 << addr.to_str() << HT_END;
    if ((error = Protocol::response_code(event)) != Error::OK)
      HT_ERROR_OUT << Error::get_text(error) << " - " << Protocol::string_format_message(event) << HT_END;
  }
  else {
    statsp = (*iter).second;
    statsp->fetch_duration = now - statsp->fetch_timestamp;
    if (event->type == Event::MESSAGE) {
      if ((error = Protocol::response_code(event)) != Error::OK) {
        statsp->fetch_error = error;
        statsp->fetch_error_msg = Protocol::string_format_message(event);
      }
      else {
        statsp->fetch_error = 0;
        statsp->fetch_error_msg = "";
        try {
          RangeServerClient::decode_response_get_statistics(event, *statsp->stats.get());
          statsp->stats_timestamp = statsp->fetch_timestamp;
        }
        catch (Exception &e) {
          statsp->fetch_error = e.code();
          statsp->fetch_error_msg = e.what();
        }
      }
    }
    else {
      statsp->fetch_error = event->error;
      statsp->fetch_error_msg = "";
    }
  }

  m_outstanding--;

  if (m_outstanding == 0)
    m_cond.notify_all();
}


void DispatchHandlerGetStatistics::wait_for_completion() {
  ScopedLock lock(m_mutex);
  while (m_outstanding > 0)
    m_cond.wait(lock);
}

