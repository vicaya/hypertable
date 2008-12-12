#!/bin/sh

HYPERTABLE_HOME=~/hypertable/current

CONFIG=$PWD/test-aol-time-order.cfg

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


while true; do
  rm count.output
  cap -S config=$CONFIG dist
  cap -S config=$CONFIG cleandb
  cap -S config=$CONFIG start

  sleep 5

  $HYPERTABLE_HOME/bin/hypertable --no-prompt --config=$CONFIG < query-log-create.hql
  if [ $? != 0 ] ; then
     echo "Unable to create table 'query-log', exiting ..."
     exit 1
  fi

  $HYPERTABLE_HOME/bin/hypertable --no-prompt --config=$CONFIG < load.hql
  if [ $? != 0 ] ; then
     echo "Problem loading table 'query-log', exiting ..."
     exit 1
  fi

  $HYPERTABLE_HOME/bin/hypertable --batch --config=$CONFIG < dump-query-log.hql > dbdump

  wc -l dbdump > count.output
  diff count.output count.golden
  if [ $? != 0 ] ; then
     echo "Test failed, exiting ..."
     exit 1
  fi
  echo "Test passed."
  if [ $RUN_ONCE == "true" ] ; then
      exit 0
  fi
done
