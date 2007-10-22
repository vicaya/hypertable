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
#include <iostream>

#include "Common/Error.h"
#include "Common/Logger.h"

#include "Hypertable/Lib/MasterClient.h"

#include "EventHandlerMasterConnection.h"

using namespace hypertable;

/**
 *
 */
void EventHandlerMasterConnection::run() {
  int error;

  cerr << "About to register with server" << endl << flush;
  if ((error = mMasterClient->RegisterServer( mServerIdStr )) != Error::OK) {
    LOG_VA_ERROR("Problem registering ourselves (%s) with the Master - %s", mServerIdStr.c_str(), Error::GetText(error));
  }
  cerr << "Just registered with server" << endl << flush;

  return;
}
