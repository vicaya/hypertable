<?php
$GLOBALS['THRIFT_ROOT'] = $_ENV['PHPTHRIFT_ROOT'];

require_once dirname(__FILE__).'/ThriftClient.php';

$client = new Hypertable_ThriftClient("localhost", 38080);
print_r($client->hql_query("show tables"));
print_r($client->hql_query("select * from thrift_test"));
