#!/usr/bin/env bash

set -euo pipefail
[ -n "${DEBUG:-}" ] && set -x
this="${BASH_SOURCE-$0}"
this_dir=$(cd -P -- "$(dirname -- "${this}")" && pwd -P)
. "${this_dir}/../config.sh"

OPTS=( "-D" "pydoop.hdfs.user=${USER}" )

nargs=1
if [ $# -ne ${nargs} ]; then
    die "Usage: $0 full|minimal"
fi
BIN="${this_dir}"/bin/wordcount_$1.py

[ ! -f "${BIN}" ] && die "${BIN} not found"

if [ $1 == "full" ]; then
    OPTS+=( "-D" "mapreduce.pipes.isjavarecordreader=false" )
fi
if [ $1 == "full" ]; then
    OPTS+=( "-D" "mapreduce.pipes.isjavarecordwriter=false" )
fi

${PYTHON} "${this_dir}"/run_wc.py "${BIN}" "${this_dir}"/../input "${OPTS[@]}"
