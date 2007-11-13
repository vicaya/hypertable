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

#ifndef HYPERTABLE_EVENT_H
#define HYPERTABLE_EVENT_H

#include <iostream>
#include <string>

extern "C" {
#include <stdint.h>
#include <netinet/in.h>
}

#include "Common/ReferenceCount.h"

using namespace std;

#include "Header.h"

namespace Hypertable {

  /**
   * Objects of the is class represent communication events.  They get passed up to
   * the application through dispatch handlers (see DispatchHandler).
   */
  class Event : public ReferenceCount {

  public:

    enum Type { CONNECTION_ESTABLISHED, DISCONNECT, MESSAGE, ERROR, TIMER };

    /** Initializes the event object with message data.  If a message header is supplied,
     * then the threadGroup member is initialized as follows:
     * <pre>
     * threadGroup = ((uint64_t)connId << 32) | header->gid;
     * </pre>
     * otherwise it is set to 0.  The thread group is used to serialize requests
     * destined for the same application object.
     *
     * @param ct type of event
     * @param cid connection ID
     * @param a remote address of connection on which this event was generated
     * @param err error code associated with this event
     * @param h pointer to message data for MESSAGE events (<b>NOTE:</b> this object
     * takes ownership of this data and deallocates it when it gets destroyed)
     */
    Event(Type ct, int cid, struct sockaddr_in &a, int err=0, Header::HeaderT *h=0) 
      : type(ct), addr(a), connId(cid), error(err), header(h) {
      if (h != 0) {
	message = ((uint8_t *)header) + header->headerLen;
	messageLen = header->totalLen - header->headerLen;
	if (header->gid != 0)
	  threadGroup = ((uint64_t)connId << 32) | header->gid;
	else
	  threadGroup = 0;
      }
      else {
	message = 0;
	messageLen = 0;
	threadGroup = 0;
      }
    }

    /** Initializes the event object with no message data.
     *
     * @param ct type of event
     * @param err error code associated with this event
     */
    Event(Type ct, int err=0) : type(ct), error(err) {
      header = 0;
      message = 0;
      messageLen = 0;
      threadGroup = 0;
      connId = 0;
    }

    /** Destroys event.  Deallocates message data
     */
    ~Event() {
      delete [] header;
    }

    /** Type of event.  Can take one of values CONNECTION_ESTABLISHED,
     * DISCONNECT, MESSAGE, ERROR, or TIMER
     */
    Type type;

    /** Remote address from which event was generated. */
    struct sockaddr_in addr;

    /** Local address to which event was delivered. */
    struct sockaddr_in localAddr;

    /** Connection ID on which this event occurred. */
    int connId;

    /** Error code associated with this event.  DISCONNECT and
     * ERROR events set this value
     */
    int error;

    /** Points to the beginning of the message header. */
    Header::HeaderT *header;

    /** Points to the beginning of the message, immediately following the header. */
    uint8_t *message;

    /** Length of the message without the header. */
    size_t messageLen;

    /** Thread group to which this message belongs.  Used to serialize
     * messages destined for the same object.  This value is created in
     * the constructor and is the combination of the connection ID and the
     * gid field in the message header:
     * <pre>
     * threadGroup = ((uint64_t)connId << 32) | header->gid;
     * </pre>
     */
    uint64_t threadGroup;

    /** Generates a one-line string representation of the event.  For example:
     * <pre>
     *   Event: type=MESSAGE protocol=hyperspace id=2 gid=0 headerLen=16 totalLen=20 from=127.0.0.1:38040
     * </pre>
     */
    std::string toString();

    /** Displays a one-line string representation of the event to stdout.  See <a href="#toString">toString</a>.
     */
    void display() { cerr << toString() << endl; }
  };

  typedef boost::intrusive_ptr<Event> EventPtr;

}

#endif // HYPERTABLE_EVENT_H
