#!/usr/bin/env bash

# BEGIN_COPYRIGHT
#
# Copyright 2009-2017 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
#
# END_COPYRIGHT

set -euo pipefail
[ -n "${DEBUG:-}" ] && set -x
this="${BASH_SOURCE-$0}"
this_dir=$(cd -P -- "$(dirname -- "${this}")" && pwd -P)
. "${this_dir}/../config.sh"

nargs=1
if [ $# -ne ${nargs} ]; then
    die "Usage: $0 prog"
fi
prog=$1

OPTS=( "--log-level" "DEBUG" "-D" "mapred.map.tasks=2" )
case ${prog} in
    base_histogram )
	DATA="${this_dir}/data/example.sam"
	;;
    transpose )
	DATA="${this_dir}/data/matrix.txt"
	OPTS+=( "--num-reducers" "4" )
	;;
    *)
	DATA="${this_dir}/../input/alice.txt"
	OPTS+=( "--num-reducers" "0" "-t" "" )
	case ${prog} in
	    caseswitch )
		OPTS+=( "-D" "caseswitch.case=upper" )
		;;
	    grep | grep_compiled )
		OPTS+=( "-D" "grep-expression=March" )
		;;
	esac
esac
INPUT=${prog}_input
OUTPUT=${prog}_output


WD=""
if [ ${prog} == grep_compiled ]; then
    WD=$(mktemp -d)
    src="${this_dir}"/scripts/grep.py
    script="${WD}"/grep.pyc
    ${PYTHON} -c "from py_compile import compile; compile('${src}', cfile='${script}')"
else
    script="${this_dir}"/scripts/${prog}.py
fi

${HADOOP} fs -rmr "/user/${USER}/${INPUT}" || :
${HADOOP} fs -mkdir -p "/user/${USER}/${INPUT}"
${HADOOP} fs -rmr "/user/${USER}/${OUTPUT}" || :
${HADOOP} fs -put "${DATA}" "${INPUT}"
${PYDOOP} script "${OPTS[@]}" "${script}" "${INPUT}" "${OUTPUT}"
${PYTHON} "${this_dir}"/check.py ${prog} "${OUTPUT}"

rm -rf "${WD}"
