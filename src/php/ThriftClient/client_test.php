<?php
$GLOBALS['THRIFT_ROOT'] = $_ENV['PHPTHRIFT_ROOT'];

require_once dirname(__FILE__).'/ThriftClient.php';

$client = new Hypertable_ThriftClient("localhost", 38080);

echo "HQL examples\n";
print_r($client->hql_query("show tables"));
print_r($client->hql_query("select * from thrift_test revs=1"));

echo "mutator examples\n";
$mutator = $client->open_mutator("thrift_test", 0, 0);
$client->set_cell($mutator, new Hypertable_ThriftGen_Cell(array(
    'row_key'=> 'php-k1', 'column_family'=> 'col', 'value'=> 'php-v1')));
$client->close_mutator($mutator, true);

echo "scanner examples\n";
$scanner = $client->open_scanner("thrift_test",
    new Hypertable_ThriftGen_ScanSpec(array('revs'=> 1)), true);

$cells = $client->next_cells($scanner);

while (!empty($cells)) {
  print_r($cells);
  $cells = $client->next_cells($scanner);
}
