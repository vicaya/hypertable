#!/usr/bin/env perl
use Hypertable::ThriftClient;
use Data::Dumper;

my $client = new Hypertable::ThriftClient("localhost", 38080);

print "HQL examples\n";
my $namespace = $client->open_namespace("test");
print Dumper($client->hql_exec($namespace,"show tables"));
print Dumper($client->hql_exec($namespace,"select * from thrift_test revs=1"));

print "mutator examples\n";
my $mutator = $client->open_mutator($namespace, "thrift_test");
my $key = new Hypertable::ThriftGen::Key({row => 'perl-k1',
                                          column_family => 'col'});
my $cell = new Hypertable::ThriftGen::Cell({key => $key,
                                            value => 'perl-v1'});
$client->set_cell($mutator, $cell);
$client->flush_mutator($mutator);

print "shared mutator examples\n";
my $mutate_spec = new Hypertable::ThriftGen::MutateSpec({appname => "test-perl", 
                                                         flush_interval => 1000, 
                                                         flags => 0});
$key = new Hypertable::ThriftGen::Key({row => 'perl-put-k1',
                                       column_family => 'col'});
$cell = new Hypertable::ThriftGen::Cell({key => $key,
                                         value => 'perl-put-v1'});
$client->put_cell($namespace, "thrift_test", $mutate_spec, $cell);

$key = new Hypertable::ThriftGen::Key({row => 'perl-put-k2',
                                       column_family => 'col'});
$cell = new Hypertable::ThriftGen::Cell({key => $key,
                                         column_family => 'col',
                                         value => 'perl-put-v2'});
$client->refresh_shared_mutator($namespace, "thrift_test", $mutate_spec);
$client->put_cell($namespace, "thrift_test", $mutate_spec, $cell);
sleep(2);

print "scanner examples\n";
my $scanner = $client->open_scanner($namespace, "thrift_test",
    new Hypertable::ThriftGen::ScanSpec({revs => 1}));

my $cells = $client->next_cells($scanner);

while (scalar @$cells) {
  print Dumper($cells);
  $cells = $client->next_cells($scanner);
}
$client->close_namespace($namespace);
