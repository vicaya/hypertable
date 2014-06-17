#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
SCRIPT_DIR=`dirname $0`

for ((i=0; i<20; i++)) ; do
    sleep .01
    $HT_HOME/bin/hypertable --batch < $SCRIPT_DIR/dump-table.hql > /dev/null
done
