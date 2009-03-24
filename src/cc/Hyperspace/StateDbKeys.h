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

#ifndef HT_HYPERSPACESTATEDBKEYS_H
#define HT_HYPERSPACESTATEDBKEYS_H

#include "Common/Compat.h"
#include <vector>

#include "Common/String.h"
#include "Common/Serialization.h"

using namespace Hypertable;
using namespace Serialization;
namespace Hyperspace {
namespace StateDbKeys {
    enum {
      //Delimeters
      PATH_DELIM                   = 0x00000001,

      //SessionMap
      SESSIONS                     = 0x40000001,
      //SessionData
      SESSION_LEASE_INTERVAL                   ,
      SESSION_ID                               ,
      SESSION_ADDR                             ,
      SESSION_HANDLES                          ,
      SESSION_EXPIRED                          ,
      SESSION_NAME                             ,

      //HandleMap
      HANDLES                      = 0x60000001,
      //Handle state
      HANDLE_ID                                ,
      HANDLE_NAME                              ,
      HANDLE_OPEN_FLAGS                        ,
      HANDLE_EVENT_MASK                        ,
      HANDLE_LOCKED                            ,
      HANDLE_NODE_NAME                         ,
      HANDLE_SESSION_ID                        ,

      //NodeMap
      NODES                        = 0x80000001,
      //Node state
      NODE_NAME                                ,
      NODE_EPHEMERAL                           ,
      NODE_LOCK_MODE                           ,
      NODE_LOCK_GENERATION                     ,
      NODE_EXCLUSIVE_LOCK_HANDLE               ,
      NODE_SHARED_LOCK_HANDLES                 ,
      NODE_PENDING_LOCK_REQUESTS               ,
      NODE_HANDLE_MAP                          ,
      NODE_HANDLE_MAP_SIZE                     ,

      //Events
      EVENTS                       = 0xa0000001,
      EVENT_ID                                 ,
      EVENT_TYPE                               ,
      EVENT_MASK                               ,
      EVENT_NAME                               ,
      EVENT_MODE                               ,
      EVENT_GENERATION                         ,
      EVENT_NOTIFICATION_HANDLES

    };

    const String PATH_DELIM_STR = "/";

    const String HANDLES_STR           = PATH_DELIM_STR + "HANDLES"
                                         + PATH_DELIM_STR;
    const String HANDLE_OPEN_FLAGS_STR = "FLAG";
    const String HANDLE_EVENT_MASK_STR = "EVT_MASK";
    const String HANDLE_LOCKED_STR     = "LCKD";
    const String HANDLE_NODE_NAME_STR  = "NODE_NAME";
    const String HANDLE_SESSION_ID_STR = "SESSN_ID";


    const String SESSIONS_STR                    = PATH_DELIM_STR +
                                                   "SESSIONS" +
                                                   PATH_DELIM_STR;
    const String SESSION_ADDR_STR                = "ADDR";
    const String SESSION_EXPIRED_STR             = "EXP";
    const String SESSION_HANDLES_STR             = "HANDLES" +
                                                   PATH_DELIM_STR;
    const String SESSION_NAME_STR                = "NAME";

    const String NODES_STR                       = PATH_DELIM_STR +
                                                   "NODES" +
                                                   PATH_DELIM_STR;
    const String NODE_EPHEMERAL_STR              = "EPHMRL";
    const String NODE_LOCK_MODE_STR              = "LK_MD";
    const String NODE_LOCK_GENERATION_STR        = "LK_GEN";
    const String NODE_EXCLUSIVE_LOCK_HANDLE_STR  = "EX_LK_HDL";
    const String NODE_SHARED_LOCK_HANDLES_STR    = "SHRD_HDLS";
    const String NODE_PENDING_LOCK_REQUESTS_STR  = "PNDG_LK_REQS";
    const String NODE_HANDLE_MAP_STR             = "HDL_MAP";
    const String NODE_HANDLE_MAP_SIZE_STR        = "HDL_MAP_SZ";


    const String EVENTS_STR = PATH_DELIM_STR + "EVENTS" + PATH_DELIM_STR;
    const String EVENT_TYPE_STR = "TYPE";
    const String EVENT_MASK_STR = "MASK";
    const String EVENT_NAME_STR = "NAME";
    const String EVENT_MODE_STR = "MODE";
    const String EVENT_GENERATION_STR = "GEN";
    const String EVENT_NOTIFICATION_HANDLES_STR = "NF_HDLS";

    const String NEXT_IDS = "/NXT_ID/";
    const String NEXT_SESSION_ID = NEXT_IDS + "SESS";
    const String NEXT_EVENT_ID   = NEXT_IDS + "EVT";
    const String NEXT_HANDLE_ID  = NEXT_IDS + "HDL";

    /**
     * Get State key for specified piece of state for specified session
     *
     * @param id Session id
     * @param key_type requested piece of state
     * @return String key for storage/retrieval in BerkeleyDB
     */
    String get_session_key(uint64_t id, uint32_t key_type);

    /**
     * Get State key to a piece of Event object
     *
     * @param id Event id
     * @param key_type  requested piece of state
     * @return String key for storage/retrieval in BerkeleyDB
     */
    String get_event_key(uint64_t id, uint32_t key_type);

    /**
     * Get State key to a piece of handle data
     *
     * @param id handle id
     * @param key_type  requested piece of state
     * @return String key for storage/retrieval in BerkeleyDB
     */
    String get_handle_key(uint64_t id, uint32_t key_type);

    /**
     * Get State key to a piece of a node state
     *
     * @param name Node name
     * @param key_type  requested piece of state
     * @return String key for storage/retrieval in BerkeleyDB
     */
    String get_node_key(const String &name, uint32_t key_type);

    /**
     * Get State key to access/store a specific node lock request
     *
     * @param name Node name
     * @param handle_id
     * @return String key for storage/retrieval in BerkeleyDB
     */
    String get_node_pending_lock_request_key(const String &name, uint64_t handle_id);


} //namespace StateDbKeys
} // namespace Hyperspace

#endif //HT_HYPERSPACESTATEDBKEYS_H
