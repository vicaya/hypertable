#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`
DATA_SIZE=${DATA_SIZE:-"100000000"}
THREADS=${THREADS:-"8"}
ITERATIONS=${ITERATIONS:-"1"}

$HT_HOME/bin/start-test-servers.sh --clear --no-thriftbroker \
    --Hypertable.RangeServer.AccessGroup.MaxFiles=2 \
    --Hypertable.RangeServer.AccessGroup.MergeFiles=2 \
    --Hypertable.RangeServer.AccessGroup.MaxMemory=2M

for ((i=0; i<$ITERATIONS; i++)) ; do

    $HT_HOME/bin/ht shell --test-mode < ${SOURCE_DIR}/initialize.hql > init.out
    if [ $? != 0 ] ; then
        echo "Iteration ${i} of rsTest failed, exiting..."
        exit 1
    fi

    for ((j=0; j<$TESTNUM; j++)) ; do

        $HT_HOME/bin/ht ht_rsclient --test-mode localhost < ${SOURCE_DIR}/Test${j}.cmd > Test${j}.output
        if [ $? != 0 ] ; then
            echo "Iteration ${i} of rsTest failed, exiting..."
            exit 1
        fi

        diff Test${j}.output ${SOURCE_DIR}/Test${j}.golden
        if [ $? != 0 ] ; then
            echo "Iteration ${i} of rsTest failed, exiting..."
            exit 1
        fi
    done
done

exit 0
