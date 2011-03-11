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

#ifndef HYPERTABLE_RANGESERVERCONNECTION_H
#define HYPERTABLE_RANGESERVERCONNECTION_H

#include <map>

#include <boost/thread/condition.hpp>

#include "AsyncComm/CommAddress.h"
#include "AsyncComm/Event.h"

#include "Common/HashMap.h"
#include "Common/InetAddr.h"
#include "Common/Mutex.h"

#include "Hypertable/Lib/MetaLogEntity.h"
#include "Hypertable/Lib/MetaLogWriter.h"

namespace Hypertable {

  namespace RangeServerConnectionState {
    enum {
      REGISTERED=0,
      PARTICIPATING=1,
      REMOVED=2
    };
  }

  class RangeServerConnection : public MetaLog::Entity {
  public:
    RangeServerConnection(MetaLog::WriterPtr &mml_writer, const String &location);
    RangeServerConnection(MetaLog::WriterPtr &mml_writer, const MetaLog::EntityHeader &header_);
    virtual ~RangeServerConnection() { }

    bool connected() { ScopedLock lock(m_mutex); return m_connected; }
    void remove();
    bool removed();
    bool wait_for_connection();
    CommAddress get_comm_address();

    virtual const String name() { return "RangeServerConnection"; }

    const String location() const { return m_location; }
    const String hostname() const { return m_hostname; }
    InetAddr local_addr() const { return m_local_addr; }
    InetAddr public_addr() const { return m_public_addr; }
    
    virtual void display(std::ostream &os);
    virtual size_t encoded_length() const;
    virtual void encode(uint8_t **bufp) const;
    virtual void decode(const uint8_t **bufp, size_t *remainp);

    friend class Context;

  protected:
    bool connect(const String &hostname, InetAddr local_addr, InetAddr public_addr);
    bool disconnect();

  private:
    Mutex m_mutex;
    boost::condition m_cond;
    MetaLog::WriterPtr m_mml_writer;
    String m_location;
    String m_hostname;
    int32_t m_state;
    time_t m_removal_time;
    CommAddress m_comm_addr;
    InetAddr m_local_addr;
    InetAddr m_public_addr;
    bool m_connected;
  };
  typedef intrusive_ptr<RangeServerConnection> RangeServerConnectionPtr;

  typedef std::map<String, RangeServerConnectionPtr> RangeServerConnectionMap;

  namespace MetaLog {
    namespace EntityType {
      enum {
        RANGE_SERVER_CONNECTION = 0x00020000
      };
    }
  }

} // namespace Hypertable

#endif // HYPERTABLE_RANGESERVERCONNECTION_H
