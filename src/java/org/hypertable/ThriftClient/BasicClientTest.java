/**
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
 *
 * This file is distributed under the Apache Software License
 * (http://www.apache.org/licenses/)
 */

package org.hypertable.ThriftClient;

import org.hypertable.thriftgen.*;

public class BasicClientTest {
  public static void main(String [] args) {
    try {
      ThriftClient client = ThriftClient.create("localhost", 38080);
      System.out.println(client.hqlQuery("show tables")
                         .toString());
      System.out.println(client.hqlQuery("select * from thrift_test")
                         .toString());
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }
}
