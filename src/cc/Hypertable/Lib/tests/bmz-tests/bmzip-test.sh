#!/bin/sh -e
BMZIP=${BMZIP:-'bmzip'}
tmpout=/tmp/bmztmp.$$
tmpcmp=/tmp/bmztmp2.$$

while [ $# -gt 0 ]; do
  case $1 in
  --fps)        fps=$2;         shift;;
  --hash)       hash=$2;        shift;;
  *)            break
  esac
  shift
done

files=${@:-'*'}
fps=${fps:-"16 32 64 128 257 512 1024"}

[ "$hash" ] && hashopt="--bm-hash $hash"

exec 2>&1

test_it() {
  fp=$1
  in=$2
  echo Begin $in fp-len=$fp
  /usr/bin/time $BMZIP $hashopt --verbose --fp-len $fp "$in" > $tmpout
  ls -l $tmpout
  /usr/bin/time $BMZIP -d $tmpout > $tmpcmp
  cmp $tmpcmp "$in"
  echo End
}

for f in $files; do
  [ -f "$f" ] || continue;
  for s in $fps; do
    test_it $s "$f"
  done
done
