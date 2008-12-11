/**
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
 *
 * This file is distributed under the Apache Software License
 * (http://www.apache.org/licenses/)
 */

package org.hypertable.ThriftClient;

import org.hypertable.thriftgen.*;

import com.facebook.thrift.TException;
import com.facebook.thrift.transport.TSocket;
import com.facebook.thrift.transport.TFramedTransport;
import com.facebook.thrift.transport.TTransportException;
import com.facebook.thrift.protocol.TBinaryProtocol;
import com.facebook.thrift.protocol.TProtocol;

public class ThriftClient extends HqlService.Client {
  public ThriftClient(TProtocol protocol) { super(protocol); }

  // Java only allow super as the first statement
  public static ThriftClient
  create(String host, int port, int timeout_ms, boolean do_open)
      throws TTransportException, TException {
    TFramedTransport transport = new TFramedTransport(new TSocket(host, port));
    ThriftClient client = new ThriftClient(new TBinaryProtocol(transport));
    client.transport = transport;

    if (do_open)
      client.open(timeout_ms);

    return client;
  }

  public static ThriftClient create(String host, int port)
      throws TTransportException, TException {
    return create(host, port, 30000, true);
  }

  public void open(int timeout_ms) throws TTransportException, TException {
    transport.open();
    do_close = true;
  }

  public void close() {
    if (do_close) {
      transport.close();
      do_close = false;
    }
  }

  public HqlResult hqlQuery(String hql) throws ClientException, TException {
    return hql_exec(hql, false, false);
  }

  private TFramedTransport transport;
  private boolean do_close = false;
}
