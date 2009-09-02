#!/bin/sh

usage() {
  echo ""
  echo "usage: make-doc-tree.sh <dst-dir>"
  echo ""
}

if [ "$#" -eq 0 ]; then
  usage
  exit 1
fi

TOPLEVEL_OUTPUT=$1
shift

if [ ! -d markdown ]; then
    echo "error: ./markdown directory not found"
    exit 1
fi

WORKING_DIR=`pwd`
BIN_DIR=$(cd `dirname "$0"` && pwd)
cd $WORKING_DIR/markdown

mkdir -p $TOPLEVEL_OUTPUT/inc
cp inc/styles.css $TOPLEVEL_OUTPUT/inc
cp -r images $TOPLEVEL_OUTPUT

for input in `find . -name '*.md'` ; do
  input=`echo $input | sed 's/^.\///g'`
  template_file=`dirname $input`/template.html
  output=$TOPLEVEL_OUTPUT/`echo $input | sed 's/.md/.html/g'`
  echo "$input -> $output"
  mkdir -p $TOPLEVEL_OUTPUT/`dirname $input`
  perl $BIN_DIR/Markdown.pl $input > tmp.html
  sed -e "/CONTENT_GOES_HERE/r tmp.html" -e "/CONTENT_GOES_HERE/d" $template_file > $output
done

rm -f tmp.html
