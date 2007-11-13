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

#ifndef HYPERTABLE_MASTERFILEHANDLER_H
#define HYPERTABLE_MASTERFILEHANDLER_H

#include "AsyncComm/ApplicationQueue.h"

#include "Hyperspace/HandleCallback.h"

#include "MasterClient.h"

using namespace Hyperspace;

namespace Hypertable {

  /**
   * 
   */
  class MasterFileHandler : public HandleCallback {
  public:
    MasterFileHandler(MasterClientPtr masterClientPtr, ApplicationQueuePtr &appQueuePtr) : HandleCallback(EVENT_MASK_ATTR_SET), m_master_client_ptr(masterClientPtr), m_app_queue_ptr(appQueuePtr) { return; }
    virtual void attr_set(std::string name);
    MasterClientPtr      m_master_client_ptr;
    ApplicationQueuePtr  m_app_queue_ptr;
  };
}

#endif // HYPERTABLE_MASTERFILEHANDLER_H
