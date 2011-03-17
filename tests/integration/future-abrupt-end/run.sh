#!/usr/bin/env bash

SCRIPT_DIR=`dirname $0`
HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
ITERATIONS=5
TEST_BIN=./future_abrupt_end_test
DATA_SIZE=50000000
KEY_SIZE=10
NUM_SCANNERS=3
INTERVALS_PER_SCANNER=4
let APPROX_NUM_ROWS=${DATA_SIZE}/1100

set -v

$HT_HOME/bin/clean-database.sh
$HT_HOME/bin/start-all-servers.sh local

cmd="$HT_HOME/bin/ht hypertable --no-prompt --command-file=$SCRIPT_DIR/create-table.hql"
echo "================="
echo "Running '${cmd}'"
echo "================="
${cmd}

cmd="$HT_HOME/bin/ht ht_load_generator update --no-log-sync --parallel=300 --spec-file=${SCRIPT_DIR}/data.spec --max-bytes=${DATA_SIZE}"
echo "================="
echo "Running '${cmd}'"
echo "================="
${cmd}

cd ${TEST_BIN_DIR};
let step=${APPROX_NUM_ROWS}/${ITERATIONS}
num_cells=0
rm future_abrupt_end_test.out
for((ii=0; $ii<${ITERATIONS}; ii=$ii+1)) do
  let num_cells=${num_cells}+${step}
  cmd="${TEST_BIN} ${KEY_SIZE} ${num_cells} ${NUM_SCANNERS} ${INTERVALS_PER_SCANNER}"
  echo "================="
  echo "Running '${cmd}'"
  echo "================="
  `${cmd}  2>&1 >> future_abrupt_end_test.out`
  if [ $? != 0 ] ; then
    echo "${cmd} failed got error %?" 
    exit 1
  fi
done

exit 0
