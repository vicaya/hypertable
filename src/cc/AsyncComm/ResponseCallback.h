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

#ifndef HYPERTABLE_RESPONSECALLBACK_H
#define HYPERTABLE_RESPONSECALLBACK_H

#include <string>

#include "Comm.h"
#include "Event.h"
#include "HeaderBuilder.h"

namespace hypertable {

  class ResponseCallback {

  public:
    ResponseCallback(Comm *comm, EventPtr &eventPtr) : m_comm(comm), m_event_ptr(eventPtr) { return; }
    virtual ~ResponseCallback() { return; }
    int error(int error, std::string msg);
    int response_ok();
    void get_address(struct sockaddr_in &addr) { memcpy(&addr, &m_event_ptr->addr, sizeof(addr)); }

  protected:
    Comm          *m_comm;
    EventPtr       m_event_ptr;
    HeaderBuilder  hbuilder_;
  };

  typedef struct {
    uint8_t *buf;
    uint32_t len;
  } ExtBufferT;

}

#endif // HYPERTABLE_RESPONSECALLBACK_H
