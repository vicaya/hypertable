/** -*- c++ -*-
 * Copyright (C) 2009 Sanjit Jhala (Zvents, Inc.)
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

#include "Common/Compat.h"
#include "Common/String.h"
#include "Common/StringExt.h"
#include "Common/Logger.h"

#include "StateDbKeys.h"

using namespace Hyperspace;
using namespace Hypertable;
using namespace Error;

namespace Hyperspace {
namespace StateDbKeys {

  String get_event_key(uint64_t id, uint32_t type)  {

    String key = EVENTS_STR + id + PATH_DELIM_STR;
    switch(type) {
      case(EVENT_ID):
        break;
      case(EVENT_TYPE):
        key += EVENT_TYPE_STR;
        break;
      case(EVENT_MASK):
        key += EVENT_MASK_STR;
        break;
      case(EVENT_NOTIFICATION_HANDLES):
        key += EVENT_NOTIFICATION_HANDLES;
        break;
      case(EVENT_NAME):
        key += EVENT_NAME_STR;
        break;
      case(EVENT_MODE):
        key += EVENT_MODE_STR;
        break;
      case(EVENT_GENERATION):
        key += EVENT_GENERATION_STR;
        break;
      default:
        HT_EXPECT(false, Error::HYPERSPACE_STATEDB_BAD_KEY);
    }
    return key;
  }

  String get_session_key(uint64_t id, uint32_t type)  {

    String key = SESSIONS_STR + id + PATH_DELIM_STR;
    switch(type) {
      case(SESSION_ID):
        break;
      case (SESSION_EXPIRED):
        key += SESSION_EXPIRED_STR;
        break;
      case(SESSION_ADDR):
        key += SESSION_ADDR_STR;
        break;
      case(SESSION_HANDLES):
        key += SESSION_HANDLES_STR;
        break;
      case(SESSION_NAME):
        key += SESSION_NAME_STR;
        break;
      default:
        HT_EXPECT(false, HYPERSPACE_STATEDB_BAD_KEY);
    }
    return key;
  }

  String get_handle_key(uint64_t id, uint32_t type)  {

    String key = HANDLES_STR + id + PATH_DELIM_STR;
    switch(type) {
      case(HANDLE_ID):
        break;
      case (HANDLE_OPEN_FLAGS):
        key += HANDLE_OPEN_FLAGS_STR;
        break;
      case(HANDLE_EVENT_MASK):
        key += HANDLE_EVENT_MASK_STR;
        break;
      case(HANDLE_LOCKED):
        key += HANDLE_LOCKED_STR;
        break;
      case(HANDLE_NODE_NAME):
        key += HANDLE_NODE_NAME_STR;
        break;
      case(HANDLE_SESSION_ID):
        key += HANDLE_SESSION_ID_STR;
        break;
      default:
        HT_EXPECT(false, Error::HYPERSPACE_STATEDB_BAD_KEY);
    }
    return key;
  }

  String get_node_key(const String &name, uint32_t type)  {

    String key = NODES_STR + name + PATH_DELIM_STR;
    switch(type) {
      case(NODE_NAME):
        break;
      case (NODE_EPHEMERAL):
        key += NODE_EPHEMERAL_STR;
        break;
      case(NODE_LOCK_GENERATION):
        key += NODE_LOCK_GENERATION_STR;
        break;
      case(NODE_LOCK_MODE):
        key += NODE_LOCK_MODE_STR;
        break;
      case(NODE_EXCLUSIVE_LOCK_HANDLE):
        key += NODE_EXCLUSIVE_LOCK_HANDLE_STR;
        break;
      case(NODE_SHARED_LOCK_HANDLES):
        key += NODE_SHARED_LOCK_HANDLES_STR;
        break;
      case(NODE_PENDING_LOCK_REQUESTS):
        key += NODE_PENDING_LOCK_REQUESTS_STR;
        break;
      case(NODE_HANDLE_MAP):
        key += NODE_HANDLE_MAP_STR;
        break;
      case(NODE_HANDLE_MAP_SIZE):
        key += NODE_HANDLE_MAP_SIZE_STR;
        break;

      default:
        HT_EXPECT(false, HYPERSPACE_STATEDB_BAD_KEY);
    }
    return key;

  }

  String get_node_pending_lock_request_key(const String &name, uint64_t handle_id)
  {
    String key = get_node_key(name, NODE_PENDING_LOCK_REQUESTS);
    key = key + handle_id;
    return key;
  }

} //namespace StateDbKeys
} //namespace Hyperspace


