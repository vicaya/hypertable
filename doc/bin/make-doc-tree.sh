#!/bin/bash

SELF=`type -p $0`
BIN_DIR=`dirname $SELF`
DOC_DIR=`dirname $BIN_DIR`/markdown

usage() {
  echo ""
  echo "usage: make-doc-tree.sh <dst-dir>"
  echo ""
}

if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

mkdir -p $1 || exit 1
TOPLEVEL_OUTPUT=`cd $1 && pwd`

if [ ! -d $DOC_DIR ]; then
    echo "error: markdown directory not found"
    exit 1
fi

cd $DOC_DIR

mkdir -p $TOPLEVEL_OUTPUT/inc
cp inc/styles.css $TOPLEVEL_OUTPUT/inc
cp -r images $TOPLEVEL_OUTPUT

for input in `find . -name '*.md'` ; do
  input=`echo $input | sed 's/^.\///g'`
  template_file=`dirname $input`/template.html
  output=$TOPLEVEL_OUTPUT/`echo $input | sed 's/.md$/.html/g'`
  echo "$input -> $output"
  mkdir -p $TOPLEVEL_OUTPUT/`dirname $input`
  perl $BIN_DIR/Markdown.pl $input > tmp.html
  sed -e "/CONTENT_GOES_HERE/r tmp.html" -e "/CONTENT_GOES_HERE/d" $template_file > $output
done

rm -f tmp.html
