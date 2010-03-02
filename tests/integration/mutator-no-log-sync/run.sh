#!/usr/bin/env bash

set -v

TEST_BIN=./MutatorNoLogSyncTest
cd ${TEST_BIN_DIR};

${TEST_BIN} $@
