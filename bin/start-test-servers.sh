#!/bin/bash

[ $# -gt 1 ] || { echo "Usage: $0 <install_dir> <file_to_touch>"; exit 1; }

INSTALL_DIR=$1
TOUCH_FILE=$2

$INSTALL_DIR/bin/clean-database.sh
$INSTALL_DIR/bin/start-all-servers.sh local && touch $TOUCH_FILE
