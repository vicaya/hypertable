#!/usr/bin/env bash
#
# Copyright (C) 2009  Luke Lu (llu@hypertable.org)
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 2
# of the License, or any later version.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Hypertable. If not, see <http://www.gnu.org/licenses/>
#
root=${1:-$(dirname `dirname $0`)}
workdir=`basename $0 .sh`-$$

dump_bt() {
  mkdir -p $workdir
  cd $workdir

  echo "thread apply all bt
  quit" > bt.gdb

  for pidfile in `ls $root/run/*.pid`; do
    server=`basename $pidfile .pid`
    case $server in
      Hyperspace)       server=Hyperspace.Master;;
      DfsBroker.local)  server=localBroker;;
    esac
    pid=`cat $pidfile`
    [ -x $root/bin/$server ] && kill -0 $pid || continue
    echo "Dumping backtraces in $server..."
    gdb -x bt.gdb $root/bin/$server `cat $pidfile` > $server-$pid.bt
  done 2>&1 | tee bt.log
}

(dump_bt)

tar zcvf $workdir.tgz $workdir
