#!/bin/sh

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"5000000"}
AG_MAXFILES=1000
AG_MAXMEM=250000

restart_servers() {
  $HT_HOME/bin/clean-database.sh
  $HT_HOME/bin/start-all-servers.sh --no-thriftbroker \
      local \
      --Hypertable.RangeServer.AccessGroup.MaxFiles=$AG_MAXFILES \
      --Hypertable.RangeServer.AccessGroup.MaxMemory=$AG_MAXMEM \
      --Hypertable.Lib.Mutator.FlushDelay=10
}

restart_servers_noclean() {
  $HT_HOME/bin/stop-servers.sh
  $HT_HOME/bin/start-all-servers.sh --no-thriftbroker \
      local \
      --Hypertable.RangeServer.AccessGroup.MaxFiles=$AG_MAXFILES \
      --Hypertable.RangeServer.AccessGroup.MaxMemory=$AG_MAXMEM \
      --Hypertable.Lib.Mutator.FlushDelay=10
}

test() {
  restart_servers

  filter_type=$1
  echo "Run test for Bloom filter of type ${filter_type}"

  $HT_HOME/bin/hypertable --no-prompt < \
      $SCRIPT_DIR/create-bloom-${filter_type}-table.hql

  echo "================="
  echo "random WRITE test"
  echo "================="
  $HT_HOME/bin/random_write_test \
      --blocksize=100 \
            $DATA_SIZE

  echo "================="
  echo "random READ test"
  echo "================="
  $HT_HOME/bin/random_read_test \
      --blocksize=100 \
      $DATA_SIZE

  restart_servers_noclean
  echo "================="
  echo "random READ test"
  echo "================="
  $HT_HOME/bin/random_read_test \
      --blocksize=100 \
      $DATA_SIZE
}

test $1
