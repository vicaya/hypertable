#!/usr/bin/env bash
#
# Copyright 2010 Doug Judd (Hypertable, Inc.)
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

JRUN_OPTS=
COUNT=1

usage() {
  echo ""
  echo "usage: start-test-client.sh [options] <host>[:<port>]"
  echo ""
  echo "options:"
  echo "  --jrun-opts <str>  Supplies <str> to the jrun command"
  echo ""
  echo "Launches the java test client.  Client will connect to the"
  echo "dispatcher running at <host>[:<port>] to receive instructions"
  echo ""
}

while [ $# -gt 1 ] ; do
  if [ "--jrun-opts" = "$1" ] ; then
    shift
    JRUN_OPTS="$JRUN_OPTS $1"
    shift
  elif [ "--count" = "$1" ] ; then
    shift
    COUNT=$1
    shift
  fi
done

if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

let j=$COUNT

while [ $j -gt 0 ] ; do
  start_server_no_check test_client jrun Hypertable.TestClient-$j $JRUN_OPTS org.hypertable.examples.PerformanceTest.Client "$@"
  let j--
done


