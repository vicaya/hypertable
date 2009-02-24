#!/usr/bin/env perl
use Hypertable::ThriftClient;
use Data::Dumper;

my $client = new Hypertable::ThriftClient("localhost", 38080);

print "HQL examples\n";
print Dumper($client->hql_exec("show tables"));
print Dumper($client->hql_exec("select * from thrift_test revs=1"));

print "mutator examples\n";
my $mutator = $client->open_mutator("thrift_test");
my $cell = new Hypertable::ThriftGen::Cell({row_key => 'perl-k1',
                                            column_family => 'col',
                                            value => 'perl-v1'});
$client->set_cell($mutator, $cell);
$client->flush_mutator($mutator);

print "scanner examples\n";
my $scanner = $client->open_scanner("thrift_test",
    new Hypertable::ThriftGen::ScanSpec({revs => 1}));

my $cells = $client->next_cells($scanner);

while (scalar @$cells) {
  print Dumper($cells);
  $cells = $client->next_cells($scanner);
}
