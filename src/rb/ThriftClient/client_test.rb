#!/usr/bin/env ruby
require File.dirname(__FILE__) + '/hypertable/thriftclient'
require 'pp'

begin
  Hypertable.with_thrift_client("localhost", 38080) do |client|
    res = client.hql_query("drop table if exists thrift_test");
    res = client.hql_query("create table thrift_test ( col )");
    res = client.hql_query("insert into thrift_test values \
                           ('2008-11-11 11:11:11', 'k1', 'col', 'v1'), \
                           ('2008-11-11 11:11:11', 'k2', 'col', 'v2'), \
                           ('2008-11-11 11:11:11', 'k3', 'col', 'v3')");
    res = client.hql_query("select * from thrift_test");
    pp res
  end
rescue Hypertable::ThriftGen::ClientException => e
  pp e;
rescue Thrift::Exception => e
  pp e;
end
