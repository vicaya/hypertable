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

namespace Hypertable {

  /**
   * This class is used to deliver responses, corresponding to a request,
   * back to a client.
   */
  class ResponseCallback {

  public:
    /**
     * Constructor. Initializes a pointer to the Comm object and saves a pointer
     * to the event that triggered the request.
     *
     * @param comm pointer to the Comm object
     * @param eventPtr smart pointer to the event that generated the request
     */
    ResponseCallback(Comm *comm, EventPtr &eventPtr) : m_comm(comm), m_event_ptr(eventPtr) { return; }

    virtual ~ResponseCallback() { return; }

    /**
     * Sends an error response back to the client.  The format of the response consists
     * of the 4-byte error code followed by the error message string.
     *
     * @param error error code
     * @param msg error message
     * @return Error::OK on success or error code on failure
     */
    int error(int error, std::string msg);

    /**
     * Sends a a simple success response back to the client which is just
     * simply the 4-byte error code Error::OK.  This can be used for all
     * requests that don't have return values.
     *
     * @return Error::OK on success or error code on failure
     */
    int response_ok();

    /** Gets the remote address of the client.
     *
     * @param addr reference to address structure to hold result
     */
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
