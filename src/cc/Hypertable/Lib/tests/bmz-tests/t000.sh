#!/bin/sh -e
BMZIP=${BMZIP:-'bmzip'}

for s in 1 16 512 1121; do
  tmp=t000.tmp$$
  $BMZIP --fp-len $s --bm-dump t000.txt > $tmp
  cmp t000.dump$s $tmp && rm -f $tmp
done
