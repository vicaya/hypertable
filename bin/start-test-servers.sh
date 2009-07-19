#!/bin/bash

[ $# -gt 1 ] || { echo "Usage: $0 <install_dir>"; exit 1; }

INSTALL_DIR=$1

$INSTALL_DIR/bin/clean-database.sh
$INSTALL_DIR/bin/start-all-servers.sh local
