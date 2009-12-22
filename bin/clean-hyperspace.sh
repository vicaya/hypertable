#!/bin/bash
#
# Copyright 2009 Sanjit Jhala(Zvents, Inc.)
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# The installation directory
export HYPERTABLE_HOME=$(cd `dirname "$0"`/.. && pwd)
. $HYPERTABLE_HOME/bin/ht-env.sh

confirm=y

if tty > /dev/null && [ $# == 1 ]; then
  case $1 in
    -i|--interactive)
      echo "Are you sure you want to clear the database (on default ports)?"
      echo "Will proceed in 10 seconds, (Ctrl-C to quit)"
      shift
      sleep 10
      ;;
  esac
fi

# Stop servers other than dfsbroker
stop_server hyperspace
sleep 1
wait_for_server_shutdown hyperspace "hyperspace" "$@" &

case $confirm in
  y|Y)
    #
    # Clear state
    #
    /bin/rm -rf $HYPERTABLE_HOME/hyperspace/*
    echo "Cleared hyperspace"
    ;;
  *) echo "Hyperspace not cleared";;
esac

wait
