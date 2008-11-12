#!/usr/bin/env perl
use Hypertable::ThriftClient;
use Data::Dumper;

my $client = new Hypertable::ThriftClient("localhost", 38080);
print Dumper($client->hql_exec("show tables"));
print Dumper($client->hql_exec("select * from thrift_test"));
