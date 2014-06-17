#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
HYPERTABLE_HOME=$HT_HOME

$HT_HOME/bin/start-test-servers.sh --no-master --no-thriftbroker --no-rangeserver --clear
cp -r $HT_HOME/conf .
