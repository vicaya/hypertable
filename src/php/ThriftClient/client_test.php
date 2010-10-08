<?php
$GLOBALS['THRIFT_ROOT'] = $_ENV['PHPTHRIFT_ROOT'];

require_once dirname(__FILE__).'/ThriftClient.php';

$client = new Hypertable_ThriftClient("localhost", 38080);
$namespace = $client->open_namespace("test");

echo "HQL examples\n";
print_r($client->hql_query($namespace, "show tables"));
print_r($client->hql_query($namespace, "select * from thrift_test revs=1 "));

echo "mutator examples\n";
$mutator = $client->open_mutator($namespace, "thrift_test", 0, 0);

$key = new Hypertable_ThriftGen_Key(array('row'=> 'php-k1', 'column_family'=> 'col'));
$cell = new Hypertable_ThriftGen_Cell(array('key' => $key, 'value'=> 'php-v1'));
$client->set_cell($mutator, $cell);
$client->close_mutator($mutator, true);

echo "shared mutator examples\n";
$mutate_spec = new Hypertable_ThriftGen_MutateSpec(array('appname'=>"test-php", 
                                                         'flush_interval'=>1000, 
                                                         'flags'=> 0));

$key = new Hypertable_ThriftGen_Key(array('row'=> 'php-put-k1', 'column_family'=> 'col'));
$cell = new Hypertable_ThriftGen_Cell(array('key' => $key, 'value'=> 'php-put-v1'));
$client->offer_cell($namespace, "thrift_test", $mutate_spec, $cell);

$key = new Hypertable_ThriftGen_Key(array('row'=> 'php-put-k2', 'column_family'=> 'col'));
$cell = new Hypertable_ThriftGen_Cell(array('key' => $key, 'value'=> 'php-put-v2'));
$client->refresh_shared_mutator($namespace, "thrift_test", $mutate_spec);
$client->offer_cell($namespace, "thrift_test", $mutate_spec, $cell);
sleep(2);

echo "scanner examples\n";
$scanner = $client->open_scanner($namespace, "thrift_test",
    new Hypertable_ThriftGen_ScanSpec(array('revs'=> 1)), true);

$cells = $client->next_cells($scanner);

while (!empty($cells)) {
  print_r($cells);
  $cells = $client->next_cells($scanner);
}
$client->close_namespace($namespace);
