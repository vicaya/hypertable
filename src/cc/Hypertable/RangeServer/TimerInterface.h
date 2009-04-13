/** -*- c++ -*-
 * Copyright (C) 2009 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERSPACE_TIMERINTERFACE_H
#define HYPERSPACE_TIMERINTERFACE_H

namespace Hypertable {

  /**
   */
  class TimerInterface : public DispatchHandler {
  public:
    virtual void handle(Hypertable::EventPtr &event_ptr) = 0;
    virtual void schedule_maintenance() = 0;
  };

}

#endif // HYPERSPACE_TIMERINTERFACE_H
