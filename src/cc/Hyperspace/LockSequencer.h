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
    /** Pathname of file that is locked */
    std::string name;
    /** lock mode (LOCK_MODE_SHARED or LOCK_MODE_EXCLUSIVE) */
    uint32_t mode;
    /* lock generation number */
    uint64_t generation;
  };

  /** Lock mode.  Used to specify the type of lock to acquire.
   * \anchor LockMode
   */
  enum {
    /** Lock in shared mode */
    LOCK_MODE_SHARED    = 1,
    /** Lock exclusive mode */
    LOCK_MODE_EXCLUSIVE = 2
  };

  /** Lock status.  Used to report the result of a lock attempt
   * \anchor LockStatus
   */
  enum {
    /** Lock successfully granted */
    LOCK_STATUS_GRANTED   = 1,
    /** Exclusive lock attempt failed because another has it locked */
    LOCK_STATUS_BUSY      = 2,
    /** Lock attempt pending (internal use only) */
    LOCK_STATUS_PENDING   = 3,
    /** Lock attempt was cancelled */
    LOCK_STATUS_CANCELLED = 4
  };
}

#endif // HYPERSPACE_LOCKSEQUENCER_H
