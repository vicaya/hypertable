#!/usr/bin/env bash

SCRIPT_DIR=`dirname $0`
HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
ITERATIONS=5
TEST_BIN=./scanner_abrupt_end_test
DATA_SIZE=50000000
let APPROX_NUM_ROWS=${DATA_SIZE}/1100

set -v

$HT_HOME/bin/clean-database.sh
$HT_HOME/bin/start-all-servers.sh local

cmd="$HT_HOME/bin/ht hypertable --no-prompt --command-file='$SCRIPT_DIR/create-table.hql'"
echo "$cmd"
${cmd}

echo "================="
echo "random WRITE test"
echo "================="
cmd="$HT_HOME/bin/ht random_write_test ${DATA_SIZE}"
echo "$cmd"
${cmd}

cd ${TEST_BIN_DIR};
let step=${APPROX_NUM_ROWS}/${ITERATIONS}
num_cells=0
rm scanner_abrupt_end_test.out
for((ii=0; $ii<${ITERATIONS}; ii=$ii+1)) do
  let num_cells=${num_cells}+${step}
  cmd="${TEST_BIN} ${num_cells}"
  echo "Running '${cmd}'"
  `${cmd}  2>&1 >> scanner_abrupt_end_test.out`
  if [ $? != 0 ] ; then
    echo "${cmd} failed got error %?" 
    exit 1
  fi
done

exit 0
