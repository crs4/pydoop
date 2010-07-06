#!/bin/bash
# BEGIN_COPYRIGHT
# END_COPYRIGHT

# You need a working HDFS installation to run this

nargs=2
if [ $# -ne $nargs ]; then
    echo "Usage: $0 TREE_DEPTH TREE_SPAN"
    exit 2
fi
DEPTH=$1
SPAN=$2

echo "Generating tree..."
python treegen.py ${DEPTH} ${SPAN} || exit 1

echo "Computing usage by block size..."
python treewalk.py
