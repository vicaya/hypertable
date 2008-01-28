#!/bin/sh
# find the best fingerprint size

[ $# -gt 0 ] || { echo "usage: $0 <file> [<report_prefix>]"; exit 1; }

BMZIP=${BMZIP:-'bmzip'}
input=$1
prefix=${2:-'bmzip'}
szout=$prefix-size-$input.out
tmout=$prefix-time-$input.out
minsz=-1
gminsz=-1

bench_it() {
  cmd=$1
  tmp=$2
  echo -n "$tmp " >&2
  eval /usr/bin/time $cmd > $tmp
  # stat is not portable enough
  sz=`ls -l $tmp | awk '{print $5}'`
  ls -l $tmp && rm -f $tmp
  [ $sz -lt $minsz -o $minsz -eq -1 ] && minsz=$sz
}

bench_em() {
  s=$1
  prefix=$input.bm$s
  minsz=-1
  bench_it "$BMZIP --fp-len $s --bm-only $input" "$prefix"
  bench_it "$BMZIP --fp-len $s $input" "$prefix.bmz"
  bench_it "$BMZIP --fp-len $s --bm-only $input | gzip -1" "$prefix.gz1"
  bench_it "$BMZIP --fp-len $s --bm-only $input | gzip" "$prefix.gz"
  bench_it "$BMZIP --fp-len $s --bm-only $input | bzip2" "$prefix.bz2"
}

main() {
  bench_it "cat $input" "$input.orig"
  bench_it "lzop -c $input" "$input.orig.lzo"
  bench_it "gzip -1 -c $input" "$input.orig.gz1"
  bench_it "gzip -c $input" "$input.orig.gz"
  bench_it "bzip2 -c $input" "$input.orig.bz2"

  # KISS here as I'm not sure that gradient-descend would work much better
  for s in 16 32 64 128 150 200 250 300 350 400 450 500 550 600 650 700 750; do
    bench_em $s
    if [ $minsz -le $gminsz -o $gminsz -eq -1 ]; then
      # use -le instead of -lt to pass occasional plateaus
      gminsz=$minsz
    else
      break
    fi
  done
}

main 2> $tmout | tee $szout
