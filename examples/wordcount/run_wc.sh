#!/usr/bin/env bash

set -euo pipefail
[ -n "${DEBUG:-}" ] && set -x
this="${BASH_SOURCE-$0}"
this_dir=$(cd -P -- "$(dirname -- "${this}")" && pwd -P)
. "${this_dir}/../config.sh"

FULL_OPTS="\
-D hadoop.pipes.java.recordreader=false
-D hadoop.pipes.java.recordwriter=false
-D pydoop.hdfs.user=${USER}"

nargs=2
if [ $# -ne ${nargs} ]; then
    die "Usage: $0 bin|old_api full|minimal"
fi
BIN="${this_dir}"/$1/wordcount_$2.py

[ ! -f "${BIN}" ] && die "${BIN} not found"
if [ $2 == "full" ]; then
    OPTS="${FULL_OPTS}"
else
    OPTS=""
fi

${PYTHON} "${this_dir}"/run_wc.py "${BIN}" "${this_dir}"/../input ${OPTS}
