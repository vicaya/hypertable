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

#ifndef HYPERSPACE_NOTIFICATION_H
#define HYPERSPACE_NOTIFICATION_H

#include "Event.h"

namespace Hyperspace {

  class Notification {
  public:
    Notification(uint64_t h, HyperspaceEventPtr &eptr) : handle(h), eventPtr(eptr) { 
      eventPtr->increment_notification_count();      
      return; 
    }
    uint64_t handle;
    HyperspaceEventPtr eventPtr;
  };

}

#endif // HYPERSPACE_NOTIFICATION_H

