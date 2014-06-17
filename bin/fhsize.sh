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

# Post install script to FHSize the running layout according FHS
# currently FHS 2.3: http://www.pathname.com/fhs/pub/fhs-2.3.html

set -e

export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
version=`basename $HYPERTABLE_HOME`

# Variable/runtime data in /var/opt
varhome=/var/opt/hypertable

echo "Setting up $varhome"

mkdir -p $varhome/hyperspace $varhome/fs $varhome/run $varhome/log
rm -rf $HYPERTABLE_HOME/{hyperspace,fs,run,log} &&
    ln -s $varhome/{hyperspace,fs,run,log} $HYPERTABLE_HOME

# Config files in /etc/opt
etchome=/etc/opt/hypertable

echo "Setting up $etchome"

mkdir -p $etchome
nfiles=`ls -1 $etchome | wc -l`
if [ $nfiles -eq 0 ] ; then
  cp $HYPERTABLE_HOME/conf/* $etchome
else
  cp $HYPERTABLE_HOME/conf/METADATA.xml $etchome
  cp $HYPERTABLE_HOME/conf/RS_METRICS.xml $etchome
fi
rm -rf $HYPERTABLE_HOME/conf && ln -s $etchome $HYPERTABLE_HOME/conf

echo ""
echo "To run Hypertable under user <username>, do something like the following:"
echo "sudo chown -R <username> $varhome $etchome"
echo ""
echo "For log rotation, install cronolog and (re)start Hypertable."
