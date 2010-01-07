#!/usr/bin/env bash
# randomly wait up to specified seconds
[ $# -gt 0 ] || { echo "Usage: $0 <max_secs>"; exit 1; }

let l=$RANDOM%$1 r=$RANDOM%1000
perl -e "select(undef,undef,undef,$l.$r);"
