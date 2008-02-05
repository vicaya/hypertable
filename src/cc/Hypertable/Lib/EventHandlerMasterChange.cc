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

extern "C" {
#include <poll.h>
}

#include "Common/Error.h"

#include "MasterClient.h"
#include "EventHandlerMasterChange.h"

using namespace Hypertable;

/**
 *
 */
void EventHandlerMasterChange::run() {
  int error;

  poll(0, 0, rand() % 3000);  // Randomly wait between 0 and 3 seconds

  if ((error = m_master_client_ptr->reload_master()) != Error::OK) {
    HT_ERRORF("Problem reloading master connection - %s", Error::get_text(error));
  }

}
