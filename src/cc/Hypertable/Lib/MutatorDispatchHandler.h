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

#ifndef HYPERTABLE_MUTATORDISPATCHHANDLER_H
#define HYPERTABLE_MUTATORDISPATCHHANDLER_H

#include "AsyncComm/DispatchHandler.h"
#include "AsyncComm/Event.h"

#include "MutatorScatterBuffer.h"

namespace Hypertable {

  /**
   * This class is a DispatchHandler class that can be used
   *
   */
  class MutatorDispatchHandler : public DispatchHandler {
  
  public:
    /**
     * Constructor.  Initializes state.
     */
    MutatorDispatchHandler(MutatorScatterBuffer::UpdateBuffer *update_buffer);

    /**
     * Dispatch method.  This gets called by the AsyncComm layer
     * when an event occurs in response to a previously sent
     * request that was supplied with this dispatch handler.
     *
     * @param eventPtr shared pointer to event object
     */
    virtual void handle(EventPtr &event_ptr);

  private:
    MutatorScatterBuffer::UpdateBuffer *m_update_buffer;
  };
}


#endif // HYPERTABLE_MUTATORDISPATCHHANDLER_H
