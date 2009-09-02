#!/bin/bash

[ $# -gt 1 ] || { echo "Usage: $0 <install_dir>"; exit 1; }

INSTALL_DIR=$1
HT_TEST_DFS=${HT_TEST_DFS:-local}

$INSTALL_DIR/bin/clean-database.sh
$INSTALL_DIR/bin/start-all-servers.sh $HT_TEST_DFS
