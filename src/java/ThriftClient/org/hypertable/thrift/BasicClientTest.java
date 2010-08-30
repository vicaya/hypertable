/**
 * Copyright (C) 2008  Luke Lu (Zvents, Inc.)
 *
 * This file is distributed under the Apache Software License
 * (http://www.apache.org/licenses/)
 */

package org.hypertable.thrift;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.HashMap;
import java.util.Iterator;

import org.hypertable.thriftgen.*;

public class BasicClientTest {
  public static void main(String [] args) {
    ThriftClient client = null;
    long ns = -1;
    try {
      client = ThriftClient.create("localhost", 38080);
      ns = client.open_namespace("test");
      // HQL examples
      show(client.hql_query(ns, "show tables").toString());
      show(client.hql_query(ns, "select * from thrift_test").toString());
      // Schema example
      Schema schema = new Schema();
      schema = client.get_schema(ns, "thrift_test");

      Iterator ag_it = schema.access_groups.keySet().iterator();
      show("Access groups:");
      while (ag_it.hasNext()) {
        show("\t" + ag_it.next());
      }

      Iterator cf_it = schema.column_families.keySet().iterator();
      show("Column families:");
      while (cf_it.hasNext()) {
        show("\t" + cf_it.next());
      }

      // mutator examples
      long mutator = client.open_mutator(ns, "thrift_test", 0, 0);

      try {
        Cell cell = new Cell();
        Key key = new Key();
        key.setRow("java-k1");
        key.setColumn_family("col");
        cell.setKey(key);
        String vtmp = "java-v1";
        cell.setValue( ByteBuffer.wrap(vtmp.getBytes()) );
        client.set_cell(mutator, cell);
      }
      finally {
        client.close_mutator(mutator, true);
      }

      // shared mutator example
      {
        MutateSpec mutate_spec = new MutateSpec();
        mutate_spec.setAppname("test-java");
        mutate_spec.setFlush_interval(1000);
        Cell cell = new Cell();
        Key key;

        key = new Key();
        key.setRow("java-put1");
        key.setColumn_family("col");
        cell.setKey(key);
        String vtmp = "java-put-v1";
        cell.setValue( ByteBuffer.wrap(vtmp.getBytes()) );
        client.offer_cell(ns, "thrift_test", mutate_spec, cell);

        key = new Key();
        key.setRow("java-put2");
        key.setColumn_family("col");
        cell.setKey(key);
        vtmp = "java-put-v2";
        cell.setValue( ByteBuffer.wrap(vtmp.getBytes()) );
        client.refresh_shared_mutator(ns, "thrift_test", mutate_spec);
        client.offer_cell(ns, "thrift_test", mutate_spec, cell);
        Thread.sleep(2000);
      }

      // scanner examples
      ScanSpec scanSpec = new ScanSpec(); // empty scan spec select all
      long scanner = client.open_scanner(ns, "thrift_test", scanSpec, true);

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

      
      // issue 497
      {
        Cell cell;
        Key key;
        String str;
        
        client.hql_query(ns, "drop table if exists java_thrift_test");
        client.hql_query(ns, "create table java_thrift_test ( c1, c2, c3 )");
        
        mutator = client.open_mutator(ns, "java_thrift_test", 0, 0);
        
        cell = new Cell();
        key = new Key();
        key.setRow("000");
        key.setColumn_family("c1");
        key.setColumn_qualifier("test");
        cell.setKey(key);
        str = "foo";
        cell.setValue( ByteBuffer.wrap(str.getBytes()) );
        client.set_cell(mutator, cell);
        
        cell = new Cell();
        key = new Key();
        key.setRow("000");
        key.setColumn_family("c1");
        cell.setKey(key);
        str = "bar";
        cell.setValue( ByteBuffer.wrap(str.getBytes()) );
        client.set_cell(mutator, cell);
        
        client.close_mutator(mutator, true);
        
        HqlResult result = client.hql_query(ns, "select * from java_thrift_test");
        List<Cell> cells = result.cells;
        int qualifier_count = 0;
        for(Cell c:cells) {
          if (c.key.isSetColumn_qualifier() && c.key.column_qualifier.length() == 0)
            qualifier_count++;
        }
        
        if (qualifier_count != 1) {
          System.out.println("ERROR: Expected qualifier_count of 1, got " + qualifier_count);
          client.close_namespace(ns);
          System.exit(1);
        }
      }

      client.close_namespace(ns);

    }
    catch (Exception e) {
      e.printStackTrace();
      try {
        if (client != null && ns != -1)
          client.close_namespace(ns);
      }
      catch (Exception ce) {
        System.err.println("Problen closing namespace \"test\" - " + e.getMessage());
      }
      System.exit(1);
    }
  }

  private static void show(String line) {
    System.out.println(line);
  }
}
