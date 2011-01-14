#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
PIDFILE=$HT_HOME/run/Hypertable.RangeServer.pid
SCRIPT_DIR=`dirname $0`

ulimit -n 1024

save_failure_state() {
  tar czvf fs-backup.tgz $HT_HOME/fs/local
  ARCHIVE_DIR="archive-"`date | sed 's/ /-/g'`
  mkdir $ARCHIVE_DIR
  mv metadata.dump fs-backup.tgz core.* select* dump.tsv rangeserver.output* error* failed* running* $ARCHIVE_DIR
  cp $HT_HOME/log/Hypertable.Master.log $ARCHIVE_DIR
  if [ -e Testing/Temporary/LastTest.log.tmp ] ; then
    mv Testing/Temporary/LastTest.log.tmp $ARCHIVE_DIR
  elif [ -e ../../../Testing/Temporary/LastTest.log.tmp ] ; then
    mv ../../../Testing/Temporary/LastTest.log.tmp $ARCHIVE_DIR
  fi
  echo "Failure state saved to directory $ARCHIVE_DIR"
  #exec 1>&-
  #sleep 86400
}

if [ -e running* ] || [ -e failed* ] ; then
  save_failure_state
  kill -9 `cat $PIDFILE`
  exit 1
fi

# clear state
/bin/rm -f core.* select* dump.tsv rangeserver.output.* error* running* report.txt

$HT_HOME/bin/start-test-servers.sh --clear --no-rangeserver --no-thriftbroker --Hypertable.Master.Gc.Interval=30000

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=11K \
    --Hypertable.RangeServer.Range.SplitSize=25K \
    --Hypertable.RangeServer.CellStore.DefaultBlockSize=1K \
    --Hypertable.RangeServer.Range.MetadataSplitSize=6K \
    --Hypertable.RangeServer.MaintenanceThreads=8 \
    --Hypertable.RangeServer.Maintenance.Interval=100 > rangeserver.output &

sleep 2

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht ht_load_generator update --spec-file=$SCRIPT_DIR/data.spec --max-bytes=300K \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=100K

sleep 5

kill -9 `cat $PIDFILE`

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$PIDFILE \
    --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=11K \
    --Hypertable.RangeServer.Range.SplitSize=25K \
    --Hypertable.RangeServer.CellStore.DefaultBlockSize=1K \
    --Hypertable.RangeServer.Range.MetadataSplitSize=6K \
    --Hypertable.RangeServer.MaintenanceThreads=8 \
    --Hypertable.RangeServer.Maintenance.Interval=100 >> rangeserver.output &

$HT_HOME/bin/ht ht_load_generator update --spec-file=$SCRIPT_DIR/data.spec --max-bytes=300K

fgrep ERROR rangeserver.output | fgrep -v " skew " > error.0
cat error.0

if [ -s error.0 ] ; then
    echo "USE sys; SELECT * FROM METADATA RETURN_DELETES DISPLAY_TIMESTAMPS;" | $HT_HOME/bin/ht shell --batch >& metadata.dump
    save_failure_state
    kill -9 `cat $PIDFILE`
    exit 1
fi

if [ -e core.* ] ; then
    echo "USE sys; SELECT * FROM METADATA RETURN_DELETES DISPLAY_TIMESTAMPS;" | $HT_HOME/bin/ht shell --batch >& metadata.dump
    save_failure_state
    kill -9 `cat $PIDFILE`
    exit 1
fi

echo "USE '/'; SELECT * FROM LoadTest INTO FILE 'select-a.0';" | $HT_HOME/bin/ht shell --batch

/bin/rm -f dump.tsv

echo "USE '/'; DUMP TABLE LoadTest INTO FILE 'dump.tsv';" | $HT_HOME/bin/ht shell --batch

kill -9 `cat $PIDFILE`

$HT_HOME/bin/ht Hypertable.RangeServer --verbose --pidfile=$PIDFILE >> rangeserver.output &

echo "USE '/'; DROP TABLE IF EXISTS LoadTest; CREATE TABLE LoadTest ( Field );" | $HT_HOME/bin/ht shell --batch

echo "USE '/'; LOAD DATA INFILE 'dump.tsv' INTO TABLE LoadTest;" | $HT_HOME/bin/ht shell --batch

echo "USE '/'; SELECT * FROM LoadTest INTO FILE 'select-b.0';" | $HT_HOME/bin/ht shell --batch

diff select-a.0 select-b.0

if [ $? != 0 ] ; then
    kill -9 `cat $PIDFILE`
    exit 1
fi

kill -9 `cat $PIDFILE`
exit 0
