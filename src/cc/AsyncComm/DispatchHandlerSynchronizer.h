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

#ifndef DISPATCHHANDLERSYNCHRONIZER_H
#define DISPATCHHANDLERSYNCHRONIZER_H

#include <queue>

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "DispatchHandler.h"
#include "Event.h"

namespace Hypertable {

  /**
   * This class is a DispatchHandler class that can be used
   * by a client to synchronize with response messages
   * resulting from previously sent request messages. It
   * contains a queue of events (response events) and a condition
   * variable that gets signalled when an event gets put on the queue.
   *
   * Example usage:
   *
   * <pre>
   * {
   *   DispatchHandlerSynchronizer syncHandler;
   *   EventPtr eventPtr;
   *   CommBufPtr cbufPtr( ... create protocol message here ...  );
   *   if ((error = m_comm->send_request(m_addr, cbufPtr, &syncHandler)) != Error::OK) {
   *      // log error message here ...
   *      return error;
   *   }
   *   if (!syncHandler.wait_for_reply(eventPtr))
   *       // log error message here ...
   *   error = (int)Protocol::response_code(eventPtr);
   *   return error;
   * } </pre>
   *
   */
  class DispatchHandlerSynchronizer : public DispatchHandler {
  
  public:
    /**
     * Constructor.  Initializes state.
     */
    DispatchHandlerSynchronizer();

    /**
     * Dispatch method.  This gets called by the AsyncComm layer
     * when an event occurs in response to a previously sent
     * request that was supplied with this dispatch handler.
     * It pushes the event onto the event queue and signals
     * (notify_one) the condition variable.
     *
     * @param eventPtr shared pointer to event object
     */
    virtual void handle(EventPtr &eventPtr);

    /**
     * This method is used by a client to synchronize.  The client
     * sends a request via the AsyncComm layer with this object
     * as the dispatch handler.  It then calls this method to
     * wait for the response (or timeout event).  This method
     * just blocks on the condition variable until the event
     * queue is non-empty and then pops and returns the head of the
     * queue.
     *
     * @param eventPtr shared pointer to event object
     * @return true if next returned event is type MESSAGE and contains status Error::OK, false otherwise
     */
    bool wait_for_reply(EventPtr &eventPtr);

  private:
    std::queue<EventPtr> m_receive_queue;
    boost::mutex         m_mutex;
    boost::condition     m_cond;
  };
}


#endif // DISPATCHHANDLERSYNCHRONIZER_H
