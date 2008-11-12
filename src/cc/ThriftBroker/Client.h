/** -*- C++ -*-
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
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
 * along with Hypertable. If not, see <http://www.gnu.org/licenses/>
 */

#ifndef HYPERTABLE_THRIFT_CLIENT_H
#define HYPERTABLE_THRIFT_CLIENT_H

// Note: do NOT add any hypertable dependencies in this file
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>

#include "gen-cpp/HqlService.h"

namespace Hypertable { namespace Thrift {

using namespace facebook::thrift;
using namespace facebook::thrift::protocol;
using namespace facebook::thrift::transport;

// helper to initialize base class of Client
struct ClientHelper {
  boost::shared_ptr<TTransport> socket;
  boost::shared_ptr<TTransport> transport;
  boost::shared_ptr<TProtocol> protocol;

  ClientHelper(const std::string &host, int port)
    : socket(new TSocket(host, port)),
      transport(new TFramedTransport(socket)),
      protocol(new TBinaryProtocol(transport)) { }
};

/**
 * A client for the ThriftBroker
 */
class Client : private ClientHelper, public ThriftGen::HqlServiceClient {
public:
  Client(const std::string &host, int port, int timeout_ms = 30000,
         bool open = true)
    : ClientHelper(host, port), HqlServiceClient(protocol) {
    if (open)
      connect(timeout_ms);
  }

  void connect(int timeout_ms) {
    // nop until thrift transport has a timeout, which it should
    transport->open();
  }

  // convenience method for common case
  void hql_query(ThriftGen::HqlResult &ret, const std::string &cmd) {
    hql_exec(ret, cmd, 0, 0);
  }
};

}} // namespace Hypertable::Thrift

#endif /* HYPERTABLE_THRIFT_CLIENT_H */
