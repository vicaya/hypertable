/**
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
 *
 * This file is distributed under the Apache Software License
 * (http://www.apache.org/licenses/)
 */

package org.hypertable.thrift;

import java.util.List;

import org.hypertable.thriftgen.*;

public class BasicClientTest {
  public static void main(String [] args) {
    try {
      ThriftClient client = ThriftClient.create("localhost", 38080);

      // HQL examples
      show(client.hql_query("show tables").toString());
      show(client.hql_query("select * from thrift_test").toString());

      // mutator examples
      long mutator = client.open_mutator("thrift_test");

      try {
        Cell cell = new Cell();
        cell.row_key = "java-k1";
        cell.column_family = "col";
        String vtmp = "java-v1";
        cell.value = vtmp.getBytes();
        client.set_cell(mutator, cell);
      }
      finally {
        client.close_mutator(mutator, true);
      }

      // scanner examples
      ScanSpec scanSpec = new ScanSpec(); // empty scan spec select all
      long scanner = client.open_scanner("thrift_test", scanSpec, true);

      try {
        List<Cell> cells = client.next_cells(scanner);

        while (cells.size() > 0) {
          show(cells.toString());
          cells = client.next_cells(scanner);
        }
      }
      finally {
        client.close_scanner(scanner);
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void show(String line) {
    System.out.println(line);
  }
}
