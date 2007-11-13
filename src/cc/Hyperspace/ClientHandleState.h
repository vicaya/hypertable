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

#ifndef HYPERSPACE_CLIENTHANDLESTATE_H
#define HYPERSPACE_CLIENTHANDLESTATE_H

#include <string>

#include <boost/thread/condition.hpp>
#include <boost/thread/mutex.hpp>

#include "Common/ReferenceCount.h"

#include "HandleCallback.h"
#include "LockSequencer.h"

namespace Hyperspace {

  class ClientHandleState : public Hypertable::ReferenceCount {
  public:
    uint64_t     handle;
    uint32_t     openFlags;
    uint32_t     eventMask;
    std::string  normalName;
    HandleCallbackPtr callbackPtr;
    struct LockSequencerT *sequencer;
    int lockStatus;
    uint32_t lockMode;
    uint64_t lockGeneration;
    boost::mutex       mutex;
    boost::condition   cond;
  };
  typedef boost::intrusive_ptr<ClientHandleState> ClientHandleStatePtr;

}

#endif // HYPERSPACE_CLIENTHANDLESTATE_H
