#!/bin/bash -e
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

export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
version=`basename $HYPERTABLE_HOME`

# Variable/runtime data in /var/opt
echo "Setting up /var/opt/hypertable..."
varhome=/var/opt/hypertable/$version
varlink=/var/opt/hypertable/current

[ -d $varhome ] && { echo "Already FHSized: $HYPERTABLE_HOME"; exit 0; }

mkdir -p $varhome/hyperspace $varhome/fs $varhome/run $varhome/log
ln -sf $varhome $varlink
ln -sf $varhome/{hyperspace,fs,run,log} $HYPERTABLE_HOME

# Config files in /etc/opt
echo "Setting up /etc/opt/hypertable..."
etchome=/etc/opt/hypertable/$version
etclink=/etc/opt/hypertable/current
mkdir -p $etchome
cp $HYPERTABLE_HOME/conf/* $etchome &&
    rm -rf $HYPERTABLE_HOME/conf && ln -s $etchome $HYPERTABLE_HOME/conf
ln -sf $etchome $etclink
