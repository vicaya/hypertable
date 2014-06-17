/**
 * Copyright (C) 2007 Sanjit Jhala (Zvents, Inc.)
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
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
 * 02110-1301, USA.
 */

#ifndef HYPERSPACE_CLIENTSTATE_H
#define HYPERSPACE_CLIENTSTATE_H

#include "Common/String.h"
#include "Common/StringExt.h"
#include "Common/HashMap.h"

namespace Hyperspace {
using namespace Hypertable;
  namespace HsClientState {
    extern int exit_status;
    extern String cwd;
    typedef hash_map<String, uint64_t> FileMap;
    extern FileMap file_map;
  }

  namespace Util {

    extern void normalize_pathname(std::string name, std::string &normal_name);
    extern uint64_t get_handle(std::string name);
  }

}

#endif // HYPERSPACE_CLIENTSTATE_H
