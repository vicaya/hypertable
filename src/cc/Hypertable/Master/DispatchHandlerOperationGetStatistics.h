/** -*- c++ -*-
 * Copyright (C) 2011 Hypertable, Inc.
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

#ifndef HYPERTABLE_DISPATCHHANDLEROPERATIONGETSTATISTICS_H
#define HYPERTABLE_DISPATCHHANDLEROPERATIONGETSTATISTICS_H

#include "AsyncComm/CommAddress.h"

#include "Common/SockAddrMap.h"
#include "Common/Timer.h"

#include "DispatchHandlerOperation.h"
#include "RangeServerStatistics.h"

namespace Hypertable {

  class DispatchHandlerOperationGetStatistics : public DispatchHandlerOperation {
  public:
    DispatchHandlerOperationGetStatistics(ContextPtr &context);
    void initialize(std::vector<RangeServerStatistics> &results);
    virtual void start(const String &location);
    virtual void result_callback(EventPtr &event);

  private:
    Timer m_timer;
    SockAddrMap<RangeServerStatistics *> m_index;
  };
  typedef intrusive_ptr<DispatchHandlerOperationGetStatistics> DispatchHandlerOperationGetStatisticsPtr;

}

#endif // HYPERTABLE_DISPATCHHANDLEROPERATIONGETSTATISTICS_H
