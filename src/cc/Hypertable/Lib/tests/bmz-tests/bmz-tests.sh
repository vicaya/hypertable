#!/bin/sh -ex

for t in t*.sh; do
  sh -ex $t
done
