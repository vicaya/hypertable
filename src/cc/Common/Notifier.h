/**
 * Copyright (C) 2008 Sanjit Jhala (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */
#ifndef HYPERTABLE_NOTIFIER_H
#define HYPERTABLE_NOTIFIER_H

#include <cstdio>
#include <string>
#include "AsyncComm/Comm.h"
#include "AsyncComm/CommBuf.h"
#include "Common/Error.h"
#include "Common/InetAddr.h"
#include "Common/ReferenceCount.h"


namespace Hypertable {
  /**
   * Helper class which sends notification to specified address.
   */
  class Notifier : public ReferenceCount {
  public:
    Notifier(const char *addr_str) {
      DispatchHandlerPtr null_handler(0);
      m_comm = Comm::instance();
      if (!InetAddr::initialize(&m_addr, addr_str)) {
        exit(1);
      }
      InetAddr::initialize(&m_send_addr, INADDR_ANY, 0);
      m_comm->create_datagram_receive_socket(&m_send_addr, 0x10, null_handler);
    }

    Notifier() : m_comm(0) {
      return;
    }

    void notify() {
      if (m_comm) {
        int error;
        CommHeader header(0);
        CommBufPtr cbp(new CommBuf(header, 0));
        if ((error = m_comm->send_datagram(m_addr, m_send_addr, cbp))
            != Error::OK) {
          HT_ERRORF("Problem sending datagram - %s", Error::get_text(error));
          exit(1);
        }
      }
    }

  private:
    Comm *m_comm;
    struct sockaddr_in m_addr;
    struct sockaddr_in m_send_addr;
  };// class Hypertable::Notifier
  typedef boost::intrusive_ptr<Notifier> NotifierPtr;

} // namepace Hypertable
#endif // HYPERTABLE_NOTIFIER_H
