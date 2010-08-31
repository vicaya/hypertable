#!/usr/bin/env bash
#
# Copyright 2010 Doug Judd (Hypertable, Inc.)
#
# This file is part of Hypertable.
#
# Hypertable is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; version 2 of the
# License, or any later version.
#
# Hypertable is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301, USA.
#

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh

usage() {
  echo ""
  echo "usage: upgrade-ok.sh <from> <to>"
  echo ""
  echo "description:"
  echo "  Determines whether or not the upgrade from Hypertable"
  echo "  version <from> to version <to> is valid.  <from> and <to>"
  echo "  are assumed to be Hypertable installation directories"
  echo "  whos last path component is either a version number or"
  echo "  the symbolic link \"current\" which points to a Hypertable"
  echo "  installation directory whos last path component is a"
  echo "  version number."
  echo ""
  echo "return:"
  echo "  Zero if upgrade is OK, non-zero otherwise"
  echo ""
}

if [ $# != 2 ] ; then
  usage
  exit 1
fi

FROM=`echo $1 | awk -F'/' '{ print $NF; }'`
TO=`echo $2 | awk -F'/' '{ print $NF; }'`

if [ "$FROM" == "current" ] ; then
  FROM=`/bin/ls -l $1 | tr -s " " | awk '{ print $NF; }' | awk -F'/' '{ print $NF; }'`
fi

if [ "$TO" == "current" ] ; then
  TO=`/bin/ls -l $2 | tr -s " " | awk '{ print $NF; }' | awk -F'/' '{ print $NF; }'`
fi

if [ "$FROM" == "$TO" ] ; then
    echo "Can't upgrade to identical version: $FROM -> $TO"
    exit 1
fi

MAJOR=`echo $FROM | cut -d'.' -f1`
MINOR=`echo $FROM | cut -d'.' -f2`
MICRO=`echo $FROM | cut -d'.' -f3`
PATCH=`echo $FROM | cut -d'.' -f4`

if [ "$MAJOR" == "" ] ||
   [ "$MINOR" == "" ] ||
   [ "$MICRO" == "" ] || 
   [ "$PATCH" == "" ] ; then
  echo "Unable to extract version number from <from> argument: $1"
  exit 1
fi

let FROM_MAJOR=$MAJOR*10000000

let FROM_MINOR=$FROM_MAJOR
let FROM_MINOR+=$MINOR*100000

let FROM_MICRO=$FROM_MINOR
let FROM_MICRO+=$MICRO*1000

let FROM_PATCH=$FROM_MICRO
let FROM_PATCH+=$PATCH

MAJOR=`echo $TO | cut -d'.' -f1`
MINOR=`echo $TO | cut -d'.' -f2`
MICRO=`echo $TO | cut -d'.' -f3`
PATCH=`echo $TO | cut -d'.' -f4`

if [ "$MAJOR" == "" ] ||
   [ "$MINOR" == "" ] ||
   [ "$MICRO" == "" ] || 
   [ "$PATCH" == "" ] ; then
  echo "Unable to extract version number from <from> argument: $1"
  exit 1
fi

let TO_MAJOR=$MAJOR*10000000

let TO_MINOR=$TO_MAJOR
let TO_MINOR+=$MINOR*100000

let TO_MICRO=$TO_MINOR
let TO_MICRO+=$MICRO*1000

let TO_PATCH=$TO_MICRO
let TO_PATCH+=$PATCH

if [ $TO_MINOR -le 1000000 ] && [ $TO_MINOR -ge 904000 ] ; then
    if [ $FROM_MINOR -le 1000000 ] && [ $FROM_MINOR -ge 904000 ] ; then
        exit 0
    fi
    echo "Incompatible upgrade: $FROM -> $TO"
    exit 1
elif [ $TO_MAJOR -ge 1000000 ] ; then
    if [ $FROM_MAJOR -ge 1000000 ] ; then
        if [ $FROM_MINOR -eq $TO_MINOR ] ; then
            exit 0
        fi
    fi
    echo "Incompatible upgrade: $FROM -> $TO"
    exit 1
elif [ $TO_MAJOR -lt 1000000 ] ; then
    if [ $FROM_MAJOR -lt 1000000 ] ; then
        if [ $FROM_MICRO -eq $TO_MICRO ] ; then
            exit 0
        fi
    fi
    echo "Incompatible upgrade: $FROM -> $TO"
    exit 1
fi

exit 0
