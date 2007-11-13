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

#ifndef HYPERTABLE_DISPATCHHANDLER_H
#define HYPERTABLE_DISPATCHHANDLER_H

#include "Common/ReferenceCount.h"

#include "Event.h"

namespace Hypertable {

  /**
   * Abstract base class that is the main callback class of the Comm layer.  An objects
   * of this class gets installed for each connection that is established to handle
   * connection related events.  An object of this class is also supplied when
   * sending a request message and is the mechanism by which the application is notified
   * of the response or a timeout.
   */
  class DispatchHandler : public ReferenceCount {
  public:

    /** Callback method.  When the Comm layer needs to deliver an event
     * to the application, this method is called to do so.  The set of event
     * types include, CONNECTION_ESTABLISHED, DISCONNECT, MESSAGE, ERROR, and TIMER.
     *
     * @param eventPtr smart pointer to Event object
     */
    virtual void handle(EventPtr &eventPtr) = 0;

    virtual ~DispatchHandler() { return; }
  };
  typedef boost::intrusive_ptr<DispatchHandler> DispatchHandlerPtr;

}

#endif // HYPERTABLE_DISPATCHHANDLER_H
