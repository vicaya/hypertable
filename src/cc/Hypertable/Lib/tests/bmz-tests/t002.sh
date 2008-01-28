#!/bin/sh -e
BMZIP=${BMZIP:-'bmzip'}

for s in 2 40 60; do
  tmp=t002.tmp$$
  $BMZIP --fp-len $s --bm-dump t002.txt > $tmp
  cmp t002.dump$s $tmp
  rm -f $tmp
done
