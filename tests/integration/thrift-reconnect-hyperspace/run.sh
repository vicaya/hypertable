#!/bin/sh -v

TEST_BIN=./client_test
HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}

$HT_HOME/bin/clean-database.sh
$HT_HOME/bin/start-all-servers.sh local --Hyperspace.Session.Reconnect=true

sleep 2;
$HT_HOME/bin/stop-servers.sh --no-thriftbroker 
$HT_HOME/bin/start-all-servers.sh --no-thriftbroker local
sleep 10;

cd ${THRIFT_CPP_TEST_DIR};
${TEST_BIN}
