/** -*- C++ -*-
 * Copyright (C) 2008 Luke Lu (Zvents, Inc.)
 *
 * This file is part of Hypertable.
 *
 * Hypertable is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License.
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

#ifndef HYPERTABLE_THRIFTBROKER_CONFIG_H
#define HYPERTABLE_THRIFTBROKER_CONFIG_H

#include "AsyncComm/Config.h"

namespace Hypertable { namespace Config {

  // init helpers
  void init_thrift_client_options();
  void init_thrift_client();
  void init_thrift_broker_options();

  struct ThriftClientPolicy : Policy {
    static void init_options() { init_thrift_client_options(); }
    static void init() { init_thrift_client(); }
  };

  /**
   * Mutually exclusive with GenericServerPolicy
   */
  struct ThriftBrokerPolicy : Policy {
    static void init_options() { init_thrift_broker_options(); }
    static void init() { init_generic_server(); }
  };

}} // namespace Hypertable::Config

#endif // HYPERTABLE_THRIFTBROKER_CONFIG_H
