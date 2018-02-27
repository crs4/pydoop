#!/usr/bin/env bash

# BEGIN_COPYRIGHT
#
# Copyright 2009-2018 CRS4.
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

# Use NLineInputFormat to force multiple mappers with a single input file
NL_INPUT_FORMAT="org.apache.hadoop.mapreduce.lib.input.NLineInputFormat"

nargs=1
if [ $# -ne ${nargs} ]; then
    die "Usage: $0 prog"
fi
prog=$1

OPTS=( "--log-level" "DEBUG" )
case ${prog} in
    base_histogram )
	DATA="${this_dir}/data/base_histogram_input"
	;;
    transpose )
	DATA="${this_dir}/data/transpose_input"
	OPTS+=( "--num-reducers" "4" "--input-format" "${NL_INPUT_FORMAT}")
	;;
    wordcount )
	DATA="${this_dir}/../input"
	OPTS+=( "--num-reducers" "2" )
	;;
    wordcount_sw )
	DATA="${this_dir}/../input"
	OPTS+=( "--num-reducers" "2" )
	OPTS+=( "--upload-file-to-cache" "${this_dir}/data/stop_words.txt" )
	;;
    wc_combiner )
	DATA="${this_dir}/../input"
	OPTS+=( "--num-reducers" "2" "-c" "combiner" )
	;;
    *)
	DATA="${this_dir}/../input"
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
INPUT="/user/${USER}/$(basename ${DATA})"
OUTPUT="/user/${USER}/${prog}_output"

WD=""
if [ ${prog} == grep_compiled ]; then
    WD=$(mktemp -d)
    src="${this_dir}"/scripts/grep.py
    script="${WD}"/grep.pyc
    ${PYTHON} -c "from py_compile import compile; compile('${src}', cfile='${script}')"
else
    script="${this_dir}"/scripts/${prog}.py
fi

${HADOOP} fs -mkdir -p "/user/${USER}"
${HADOOP} fs -rm -r "${INPUT}" "${OUTPUT}" || :
${HADOOP} fs -put "${DATA}" "${INPUT}"
${PYDOOP} script "${OPTS[@]}" "${script}" "${INPUT}" "${OUTPUT}"
${PYTHON} "${this_dir}"/check.py ${prog} "${OUTPUT}"

rm -rf "${WD}"
