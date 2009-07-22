#!/bin/sh

if [ "$#" -lt 1 ]; then
  echo "usage: run.sh <max-bytes>"
  exit 1
fi

MAX_BYTES=$1

HYPERTABLE_HOME=/home/doug/hypertable/current

$HYPERTABLE_HOME/bin/clean-database.sh
$HYPERTABLE_HOME/bin/start-all-servers.sh local

$HYPERTABLE_HOME/bin/hypertable < create-table.hql

sleep 2

$HYPERTABLE_HOME/bin/ht_load_generator update --spec-file=data.spec --max-bytes=$MAX_BYTES --rowkey.seed=1 >& 1.out

$HYPERTABLE_HOME/bin/ht_load_generator query --spec-file=data.spec --max-bytes=100000 --rowkey.seed=1 >& query.out
