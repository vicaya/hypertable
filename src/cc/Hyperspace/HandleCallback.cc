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

#include "HandleCallback.h"

namespace Hyperspace {

  const char *EventMaskToString(uint32_t mask) {
    if (mask == EVENT_MASK_ATTR_SET)
      return "EVENT_MASK_ATTR_SET";
    else if (mask == EVENT_MASK_ATTR_DEL)
      return "EVENT_MASK_ATTR_DEL";
    else if (mask == EVENT_MASK_CHILD_NODE_ADDED)
      return "EVENT_MASK_CHILD_NODE_ADDED";
    else if (mask == EVENT_MASK_CHILD_NODE_REMOVED)
      return "EVENT_MASK_CHILD_NODE_REMOVED";
    else if (mask == EVENT_MASK_LOCK_ACQUIRED)
      return "EVENT_MASK_LOCK_ACQUIRED";
    else if (mask == EVENT_MASK_LOCK_RELEASED)
      return "EVENT_MASK_LOCK_RELEASED";
    else if (mask == EVENT_MASK_LOCK_GRANTED)
      return "EVENT_MASK_LOCK_GRANTED";
    return "UNKNOWN";
  }

}
