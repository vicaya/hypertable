#!/bin/bash -ex

for t in t*.sh; do
  bash -ex $t
done
