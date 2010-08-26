#!/bin/bash

nargs=1
if [ $# -lt ${nargs} ]; then
    echo "USAGE: $(basename $0) INPUT_DIR"
    exit 2
fi
INPUT_DIR=$1

bash run.sh bin/ipcount_base.py ${INPUT_DIR}
