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

#ifndef HYPERSPACE_SESSIONHANDLER_H
#define HYPERSPACE_SESSIONHANDLER_H

#include <iostream>

#include "Hyperspace/Session.h"

namespace Hypertable {

  class HyperspaceSessionHandler : public Hyperspace::SessionCallback {
  public:
    virtual void safe() {
      std::cout << "Hyperspace session state = SAFE" << std::endl;
    }
    virtual void expired() {
      std::cout << "Hyperspace session state = EXPIRED" << std::endl;
    }
    virtual void jeopardy() {
      std::cout << "Hyperspace session state = JEOPARDY" << std::endl;
    }
    virtual void disconnected() {
      std::cout << "Hyperspace session state = RECONNECTING" << std::endl;
    }
    virtual void reconnected() {
      std::cout << "Hyperspace session state = RECONNECTED" << std::endl;
    }

  };

}

#endif // HYPERSPACE_SESSIONHANDLER_H

