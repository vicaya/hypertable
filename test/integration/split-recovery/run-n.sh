#!/bin/bash

[ $# -gt 0 ] || { echo "$0 <n>"; exit 1; }

n=$1
i=1

while [ $i -le $n ]; do
  rundir=result$i
  mkdir -p $rundir
  (cd $rundir && time sh -x ../run.sh > run.out 2>&1)
  i=$((i + 1))
done
