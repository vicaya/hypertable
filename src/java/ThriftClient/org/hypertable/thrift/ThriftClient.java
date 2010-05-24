/**
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
 *
 * This file is distributed under the Apache Software License
 * (http://www.apache.org/licenses/)
 */

package org.hypertable.thrift;

import org.hypertable.thriftgen.*;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;

public class ThriftClient extends HqlService.Client {
  public ThriftClient(TProtocol protocol) { super(protocol); }

  // Java only allow super as the first statement of constructor.  It doesn't
  // support multiple inheritance to use the base-from-member idiom either. So,
  // we're resorting to a static factory method here.
  public static ThriftClient
  create(String host, int port, int timeout_ms, boolean do_open)
      throws TTransportException, TException {
    TFramedTransport transport = new TFramedTransport(
        new TSocket(host, port, timeout_ms));
    ThriftClient client = new ThriftClient(new TBinaryProtocol(transport));
    client.transport = transport;

    if (do_open)
      client.open();

    return client;
  }

  // Java doesn't support default argument values, which makes things
  // unnecessarily verbose here
  public static ThriftClient create(String host, int port)
      throws TTransportException, TException {
    return create(host, port, 300000, true);
  }

  public void open() throws TTransportException, TException {
    transport.open();
    do_close = true;
  }

  public void close() {
    if (do_close) {
      transport.close();
      do_close = false;
    }
  }

  private TFramedTransport transport;
  private boolean do_close = false;
}
