#!/usr/bin/env ruby
require File.dirname(__FILE__) + '/hypertable/thrift_client'
require 'pp'
include Hypertable::ThriftGen;

begin
  Hypertable.with_thrift_client("localhost", 38080) do |client|
    puts "testing hql queries..."
    ns = client.open_namespace("test");
    res = client.hql_query(ns, "drop table if exists thrift_test");
    res = client.hql_query(ns, "create table thrift_test ( col )");
    res = client.hql_query(ns, "insert into thrift_test values \
                           ('2008-11-11 11:11:11', 'k1', 'col', 'v1'), \
                           ('2008-11-11 11:11:11', 'k2', 'col', 'v2'), \
                           ('2008-11-11 11:11:11', 'k3', 'col', 'v3')");
    res = client.hql_query(ns, "select * from thrift_test");
    pp res

    puts "testing scanner api..."
    # testing scanner api
    client.with_scanner(ns, "thrift_test", ScanSpec.new()) do |scanner|
      client.each_cell(scanner) { |cell| pp cell }
    end
    
    puts "testing asynchronous api..."
    # testing asynchronous scanner api
    client.with_future() do |future|
      color_scanner = client.open_scanner_async(ns, "FruitColor", future, ScanSpec.new(), true)
      location_scanner = client.open_scanner_async(ns, "FruitLocation", future, ScanSpec.new(), true)
      energy_scanner = client.open_scanner_async(ns, "FruitEnergy", future, ScanSpec.new(), true)
      expected_cells=6
      num_cells=0
      while (true)
        result = client.get_future_result(future)
        if result.is_empty || result.is_error || !result.is_scan
          break
        end
        result.cells.each do |cell|
          pp cell
          num_cells = num_cells+1
        end
        if (num_cells >= 6)
          client.cancel_future(future)
          break
        end
      end
      client.close_scanner_async(color_scanner)
      client.close_scanner_async(location_scanner)
      client.close_scanner_async(energy_scanner)
      if num_cells != expected_cells
        puts "Expected #{expected_cells} got {#num_cells}"
        exit 1
      end
    end

    puts "testing mutator api..."
    client.with_mutator(ns, "thrift_test") do |mutator|
      key = Key.new
      key.row = "k4"
      key.column_family = "col"
      key.timestamp = 1226401871000000000; # 2008-11-11 11:11:11
      cell = Cell.new
      cell.key = key
      cell.value = "v4"
      client.set_cell(mutator, cell);
    end
    
    puts "testing shared mutator api..."
    
    mutate_spec = MutateSpec.new
    mutate_spec.appname = "test-ruby"
    mutate_spec.flush_interval = 1000
    mutate_spec.flags = 0

    key = Key.new
    key.row = "ruby-put-k1"
    key.column_family = "col"
    key.timestamp = 1226401871000000000; # 2008-11-11 11:11:11
    cell = Cell.new
    cell.key = key
    cell.value = "ruby-put-v1"
    client.offer_cell(ns, "thrift_test", mutate_spec, cell);

    key = Key.new
    key.row = "ruby-put-k2"
    key.column_family = "col"
    key.timestamp = 1226401871000000000; # 2008-11-11 11:11:11
    cell = Cell.new
    cell.key = key
    cell.value = "ruby-put-v2"
    client.refresh_shared_mutator(ns, "thrift_test", mutate_spec);
    client.offer_cell(ns, "thrift_test", mutate_spec, cell);
   
    client.with_mutator(ns, "thrift_test") do |mutator|
      key = Key.new
      key.row = "k4"
      key.column_family = "col"
      key.timestamp = 1226401871000000000; # 2008-11-11 11:11:11
      cell = Cell.new
      cell.key = key
      cell.value = "v4"
      client.set_cell(mutator, cell);
    end
    
    puts "checking for k4..."
    pp client.hql_query(ns, "select * from thrift_test where row > 'k3'")
    client.close_namespace(ns)
  end
rescue
  pp $!
  exit 1
end
