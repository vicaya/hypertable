/** -*- c++ -*-
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERTABLE_EVENT_H
#define HYPERTABLE_EVENT_H

#include <iostream>
#include <time.h>

#include "Common/InetAddr.h"
#include "Common/String.h"
#include "Common/ReferenceCount.h"

#include "CommHeader.h"

namespace Hypertable {

  /**
   * Objects of the is class represent communication events.  They get passed
   * up to the application through dispatch handlers (see DispatchHandler).
   */
  class Event : public ReferenceCount {

  public:
    enum Type { CONNECTION_ESTABLISHED, DISCONNECT, MESSAGE, ERROR, TIMER };

    /** Initializes the event object.
     *
     * @param ct type of event
     * @param a remote address from which event originated
     * @param err error code associated with this event
     */
    Event(Type ct, const sockaddr_in &a, int err = 0)
      : type(ct), addr(a), proxy_buf(0), error(err), payload(0), payload_len(0),
        thread_group(0), arrival_clocks(0), arrival_time(0) {
      proxy = 0;
    }

    /** Initializes the event object.
     *
     * @param ct type of event
     * @param a remote address from which event originated
     * @param p address proxy
     * @param err error code associated with this event
     */
    Event(Type ct, const sockaddr_in &a, const String &p, int err = 0)
      : type(ct), addr(a), proxy_buf(0), error(err), payload(0), payload_len(0),
        thread_group(0), arrival_clocks(0), arrival_time(0) {
      set_proxy(p);
    }

    /** Initializes the event object.
     *
     * @param ct type of event
     * @param err error code associated with this event
     */
    Event(Type ct, int err=0) : type(ct), proxy_buf(0), error(err), payload(0),
        payload_len(0), thread_group(0), arrival_clocks(0), arrival_time(0) {
      proxy = 0;
    }

    /** Initializes the event object.
     *
     * @param ct type of event
     * @param err error code associated with this event
     */
    Event(Type ct, const String &p, int err=0) : type(ct), proxy_buf(0),
          error(err), payload(0), payload_len(0), thread_group(0),
	  arrival_clocks(0), arrival_time(0) {
      set_proxy(p);
    }

    /** Destroys event.  Deallocates message data
     */
    ~Event() {
      delete [] payload;
      delete [] proxy_buf;
    }

    /** Loads header object from serialized buffer.  This method
     * also sets the thread_group member.
     *
     * @param sd socket descriptor from which the event was generated
     *        (used for thread_group)
     * @param buf buffer containing serialized header
     * @param len length of buffer
     */
    void load_header(int sd, const uint8_t *buf, size_t len) {
      header.decode(&buf, &len);
      if (header.gid != 0)
        thread_group = ((uint64_t)sd << 32) | header.gid;
      else
        thread_group = 0;
    }

    void set_proxy(const String &p) {
      if (p.length() == 0)
	proxy = 0;
      else {
	if (p.length() < 32)
	  proxy = proxy_buf_static;
	else {
	  proxy_buf = new char [ p.length() + 1 ];
	  proxy = proxy_buf;
	}
	strcpy((char *)proxy, p.c_str());
      }
    }

    /** Type of event.  Can take one of values CONNECTION_ESTABLISHED,
     * DISCONNECT, MESSAGE, ERROR, or TIMER
     */
    Type type;

    /** Remote address from which event was generated. */
    InetAddr addr;

    /** Address proxy */
    const char *proxy;
    char *proxy_buf;
    char proxy_buf_static[32];

    /** Local address to which event was delivered. */
    struct sockaddr_in local_addr;

    /** Error code associated with this event.  DISCONNECT and
     * ERROR events set this value
     */
    int error;

    /** Comm layer header for MESSAGE events */
    CommHeader header;

    /** Points to a buffer containing the message payload */
    const uint8_t *payload;

    /** Length of the message */
    size_t payload_len;

    /** Thread group to which this message belongs.  Used to serialize
     * messages destined for the same object.  This value is created in
     * the constructor and is the combination of the socked descriptor from
     * which the message was read and the gid field in the message header:
     * <pre>
     * thread_group = ((uint64_t)sd << 32) | header->gid;
     * </pre>
     * If the gid is zero, then the thread_group member is also set to zero
     */
    uint64_t thread_group;

    /** time (clock ticks) when message arrived **/
    clock_t arrival_clocks;

    /** time (seconds since epoch) when message arrived **/
    time_t arrival_time;

    /** Generates a one-line string representation of the event.  For example:
     * <pre>
     *   Event: type=MESSAGE id=2 gid=0 header_len=16 total_len=20 \
     *   from=127.0.0.1:38040 ...
     * </pre>
     */
    String to_str();

    /** Displays a one-line string representation of the event to stdout.
     * @see to_str
     */
    void display() { std::cerr << to_str() << std::endl; }
  };

  typedef boost::intrusive_ptr<Event> EventPtr;

} // namespace Hypertable

#endif // HYPERTABLE_EVENT_H
