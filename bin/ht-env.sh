#!/bin/bash
exec 2>&1

ulimit -c unlimited

die() {
  echo "$@"
  exit 1
}

stop_server() {
  for pidfile in $@; do
    if [ -f $pidfile ] ; then
      pid=`cat $pidfile`
      echo "Killing `basename $pidfile` $pid"
      kill -9 $pid
      rm $pidfile
    fi
  done
}

wait_for_server_shutdown() {
  server=$1
  serverdesc=$2
  $HYPERTABLE_HOME/bin/serverup --silent $server
  while [ $? == 0 ] ; do
    echo "Waiting for $serverdesc to shutdown ..."
    sleep 1
    $HYPERTABLE_HOME/bin/serverup --silent $server
  done
  echo "Shutdown $2 complete"
}

[ "$HYPERTABLE_HOME" ] || die "Expected HYPERTABLE_HOME is not set"

export LD_LIBRARY_PATH="$HYPERTABLE_HOME/lib:$LD_LIBRARY_PATH"
export DYLD_LIBRARY_PATH="$LD_LIBRARY_PATH:$DYLD_LIBRARY_PATH"
