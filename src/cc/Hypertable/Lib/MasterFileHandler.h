/** -*- c++ -*-
 * Copyright (C) 2008 Doug Judd (Zvents, Inc.)
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

#ifndef HYPERTABLE_MASTERFILEHANDLER_H
#define HYPERTABLE_MASTERFILEHANDLER_H

#include "AsyncComm/ApplicationQueue.h"

#include "Hyperspace/HandleCallback.h"

#include "MasterClient.h"


namespace Hypertable {

  using namespace Hyperspace;
  /**
   *
   */
  class MasterFileHandler : public Hyperspace::HandleCallback {
  public:

    MasterFileHandler(MasterClient *master_client,
                      ApplicationQueuePtr &app_queue)
      : HandleCallback(EVENT_MASK_ATTR_SET), m_master_client(master_client),
        m_app_queue_ptr(app_queue) { }

    virtual void attr_set(std::string name);
    MasterClient         *m_master_client;
    ApplicationQueuePtr   m_app_queue_ptr;
  };
}

#endif // HYPERTABLE_MASTERFILEHANDLER_H
