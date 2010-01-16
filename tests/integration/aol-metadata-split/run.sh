#!/bin/sh

HYPERTABLE_HOME=/opt/hypertable/current

CONFIG=$PWD/test-aol-metadata-split.cfg

RUN_ONCE="true"

while [ "$1" != "${1##[-+]}" ]; do
    case $1 in
        --repeat)
            RUN_ONCE="false"
            shift
            ;;
        *)
            echo $"$0: Invalid option - $1"
            exit 1;;
    esac
done

DFS="hadoop"
if [ "$#" -ne 0 ]; then
  DFS=$1
  shift
fi

while true; do
  rm count.output
  cap -S config=$CONFIG -S dfs=$DFS dist
  cap -S config=$CONFIG -S dfs=$DFS stop
  cap -S config=$CONFIG -S dfs=$DFS cleandb
  cap -S config=$CONFIG -S dfs=$DFS start

  sleep 5

  $HYPERTABLE_HOME/bin/ht shell --no-prompt --config=$CONFIG \
      < query-log-create.hql
  if [ $? != 0 ] ; then
     echo "Unable to create table 'query-log', exiting ..."
     exit 1
  fi

  $HYPERTABLE_HOME/bin/ht shell --no-prompt --config=$CONFIG < load.hql
  if [ $? != 0 ] ; then
     echo "Problem loading table 'query-log', exiting ..."
     exit 1
  fi

  $HYPERTABLE_HOME/bin/ht shell --batch --config=$CONFIG \
      < dump-query-log.hql > dbdump
  wc -l dbdump > count.output
  diff count.output count.golden
  if [ $? != 0 ] ; then
     echo "Test failed, exiting ..."
     exit 1
  fi

  cap -S dumpfile="/tmp/rsdump-before.txt" -S config=$CONFIG rangeserver_dump
  cap -S config=$CONFIG -S dfs=$DFS stop
  cap -S config=$CONFIG -S dfs=$DFS start

  sleep 5

  time $HYPERTABLE_HOME/bin/ht shell --batch --config=$CONFIG \
      < dump-query-log.hql > dbdump

  cap -S dumpfile="/tmp/rsdump-after.txt" -S config=$CONFIG rangeserver_dump

  wc -l dbdump > count.output
  diff count.output count.golden
  if [ $? != 0 ] ; then
      echo "Test failed (recovery), exiting ..."
      exit 1
  fi

  echo "Test passed."
  if [ $RUN_ONCE == "true" ] ; then
      exit 0
  fi
done
