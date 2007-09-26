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

#ifndef HYPERSPACE_LOCKSEQUENCER_H
#define HYPERSPACE_LOCKSEQUENCER_H

#include <string>

namespace Hyperspace {

  /**
   * Lock sequencer.  This object gets created with each lock
   * acquisition and gets passed to each service that expects
   * to be protected by the lock.  The service will check the
   * validity of this sequencer with a call to CheckSequencer
   * and will reject requests if the sequencer is no longer valid.
   */
  struct LockSequencerT {
    std::string name;
    uint32_t mode;
    uint64_t generation;
  };

  enum {
    LOCK_MODE_SHARED    = 1,
    LOCK_MODE_EXCLUSIVE = 2
  };

  enum {
    LOCK_STATUS_GRANTED   = 1,
    LOCK_STATUS_BUSY      = 2,
    LOCK_STATUS_PENDING   = 3,
    LOCK_STATUS_CANCELLED = 4
  };
}

#endif // HYPERSPACE_LOCKSEQUENCER_H
