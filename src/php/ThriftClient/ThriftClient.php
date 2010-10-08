<?php
//$GLOBALS['THRIFT_ROOT'] = '/Users/luke/Source/thrift/lib/php/src';
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TFramedTransport.php';

/**
 * Suppress errors in here, which happen because we have not installed into
 * $GLOBALS['THRIFT_ROOT'].'/packages/tutorial' like they assume!
 *
 * Normally we would only have to include HqlService.php which would properly
 * include the other files from their packages/ folder locations, but we
 * include everything here due to the bogus path setup.
 */
$old_error_reporting = error_reporting();
error_reporting(E_NONE);
$GEN_DIR = dirname(__FILE__).'/gen-php';
require_once $GEN_DIR.'/Client/ClientService.php';
require_once $GEN_DIR.'/Client/Client_types.php';
require_once $GEN_DIR.'/Hql/HqlService.php';
require_once $GEN_DIR.'/Hql/Hql_types.php';
error_reporting($old_error_reporting);

class Hypertable_ThriftClient extends HqlServiceClient {
  function __construct($host, $port, $timeout_ms = 300000, $do_open = true) {
    $socket = new TSocket($host, $port);
    $socket->setSendTimeout($timeout_ms);
    $socket->setRecvTimeout($timeout_ms);
    $this->transport = new TFramedTransport($socket);
    $protocol = new TBinaryProtocol($this->transport);
    parent::__construct($protocol);

    if ($do_open)
      $this->open();
  }

  function __destruct() { $this->close(); }

  function open() {
    $this->transport->open();
    $this->do_close = true;
  }

  function close() {
    if ($this->do_close)
      $this->transport->close();
  }
}
