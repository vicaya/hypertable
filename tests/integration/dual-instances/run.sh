#!/usr/bin/env bash

HT_HOME=${INSTALL_DIR:-"$HOME/hypertable/current"}
ALT_HOME=alt/current
HYPERTABLE_HOME=$HT_HOME
SCRIPT_DIR=`dirname $0`
WRITE_TOTAL=${WRITE_TOTAL:-"30000000"}

. $HT_HOME/bin/ht-env.sh

if [ ! -e words ]; then
  gzip -d < ${TEST_DATA_DIR}/words.gz > words
fi

$HT_HOME/bin/start-test-servers.sh --clear \
   --Hypertable.RangeServer.Maintenance.Interval 100 \
   --Hypertable.RangeServer.Range.SplitSize=1M

CURPWD=`pwd`
echo "executing rm -rf $ALT_HOME in $CURPWD"
rm -rf $ALT_HOME
mkdir -p $ALT_HOME
cp -r $HT_HOME/bin $ALT_HOME
ln -s $HT_HOME/conf $ALT_HOME/conf
ln -s $HT_HOME/lib $ALT_HOME/lib

env INSTALL_DIR=$ALT_HOME $ALT_HOME/bin/start-test-servers.sh --clear --config=$SCRIPT_DIR/alt_instance.cfg

$HT_HOME/bin/ht shell --no-prompt < $SCRIPT_DIR/create-table.hql

$HT_HOME/bin/ht_load_generator update \
        --Hypertable.Mutator.ScatterBuffer.FlushLimit.PerServer=10K \
        --rowkey.component.0.type=integer \
        --rowkey.component.0.order=random \
        --rowkey.component.0.format="%010lld" \
        --Field.value.size=10 \
        --Field.value.source=words \
        --max-bytes=$WRITE_TOTAL


echo "select * from LoadTest;" | $HT_HOME/bin/ht shell --batch > intermediate.tsv

$ALT_HOME/bin/ht shell --batch --config=$SCRIPT_DIR/alt_instance.cfg < $SCRIPT_DIR/create-table.hql

echo "load data infile 'intermediate.tsv' into TABLE LoadTest;" | $ALT_HOME/bin/ht shell --batch --config=$SCRIPT_DIR/alt_instance.cfg

echo "select * from LoadTest;" | $ALT_HOME/bin/ht shell --batch --config=$SCRIPT_DIR/alt_instance.cfg > final.tsv

$ALT_HOME/bin/clean-database.sh --config=$SCRIPT_DIR/alt_instance.cfg

diff intermediate.tsv final.tsv > /dev/null
