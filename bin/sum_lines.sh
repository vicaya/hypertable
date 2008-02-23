#!/bin/sh

cat $@ | awk 'BEGIN { tot=0; } { tot += $1; } END { print tot; }'
