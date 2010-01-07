#!/usr/bin/env bash
INSTALL_DIR=${INSTALL_DIR:-$(cd `dirname $0`/.. && pwd)}
HT_TEST_DFS=${HT_TEST_DFS:-local}

usage_exit() {
  echo "$0 [Options]"
  echo ""
  echo "Options:"
  echo "  --clear               Clear any existing data"
  echo "  -h, --help            Show this help message"
  echo "  and any valid start-all-servers.sh options"
}

while [ $# -gt 0 ]; do
  case $1 in
    --clear)            clear=1;;
    -h|--help)          usage_exit;;
    --val*|--no*)       opts[${#opts[*]}]=$1;;
    *)                  break;;
  esac
  shift
done

if [ "$clear" ]; then
  $INSTALL_DIR/bin/ht clean-database
else
  $INSTALL_DIR/bin/ht stop servers
fi

$INSTALL_DIR/bin/ht start all-servers "${opts[@]}" $HT_TEST_DFS "$@"
