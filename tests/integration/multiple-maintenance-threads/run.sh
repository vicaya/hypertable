#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"100000000"}
THREADS=${THREADS:-"8"}
ITERATIONS=${ITERATIONS:-"1"}

for ((i=0; i<$ITERATIONS; i++)) ; do
    $HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
        --Hypertable.RangeServer.Range.SplitSize=2500K \
        --Hypertable.RangeServer.AccessGroup.MaxMemory=400K \
        --Hypertable.RangeServer.MaintenanceThreads=$THREADS \
        --Hypertable.RangeServer.Maintenance.Interval=10k \
        --Hypertable.RangeServer.AccessGroup.CellCache.PageSize=5k

    $HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

    $SCRIPT_DIR/dump-loop.sh &

    echo "=================================="
    echo "$THREADS Maintenence Threads WRITE test"
    echo "=================================="
    $HT_HOME/bin/ht random_write_test $DATA_SIZE

    kill %1

    dump_it() {
        $HT_HOME/bin/ht shell --batch < $SCRIPT_DIR/dump-table.hql | wc -l
    }

    count=`dump_it`
    expected=$((DATA_SIZE / 1000))

    if test $count -ne $expected; then
        echo "Expected: $expected, got: $count"
        # try dump again to see if it's a transient problem
        echo "Second dump: `dump_it`"
        #exec 1>&-
        #sleep 86400
        exit 1
    fi
done

exit 0
