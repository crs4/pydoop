#!/usr/bin/env bash

# BEGIN_COPYRIGHT
#
# Copyright 2009-2019 CRS4.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not
# use this file except in compliance with the License. You may obtain a copy
# of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
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

OPTS=(
    "-D" "mapreduce.task.timeout=10000"
    "-D" "mapreduce.job.maps=2"
    "--python-program" "${PYTHON}"
)

while getopts ":p:" opt; do
    case ${opt} in
    p )
	OPTS+=( "--pstats-dir" "${OPTARG}" )
	OPTS+=( "--pstats-fmt" "_test_%s_%05d_%s" )
	;;
    \? )
	echo "Invalid option: -${OPTARG}" >&2
	exit 1
	;;
    : )
	echo "Option -${OPTARG} requires an argument" >&2
	exit 1
	;;
    esac
done
shift $((${OPTIND} - 1))

nargs=1
if [ $# -ne ${nargs} ]; then
    die "Usage: $0 [-p PSTATS_DIR] MODULE_NAME"
fi
MODULE=$1

APP_DIR="${this_dir}/mr"
JOBNAME=${MODULE}
RESULTS=results.txt

OPTS+=( "--job-name" "${JOBNAME}" )
case ${MODULE} in
    wordcount_minimal )
	DATA="${this_dir}"/../input
	OPTS+=("--entry-point" "main")
	;;
    wordcount_full )
	DATA="${this_dir}"/../input
	OPTS+=("--entry-point" "main")
	OPTS+=( "--do-not-use-java-record-reader" )
	OPTS+=( "--do-not-use-java-record-writer" )
	OPTS+=( "-D" "pydoop.hdfs.user=${USER}" )
	;;
    nosep )
	DATA="${this_dir}"/data
	OPTS+=( "--num-reducers" "0" )
	OPTS+=( "--output-format" "it.crs4.pydoop.NoSeparatorTextOutputFormat" )
	;;
    map_only_java_writer )
	DATA="${this_dir}"/../input
	OPTS+=( "--num-reducers" "0" )
	;;
    map_only_python_writer )
	DATA="${this_dir}"/../input
	OPTS+=( "--num-reducers" "0" )
	OPTS+=( "--do-not-use-java-record-writer" )
	;;
esac
OPTS+=( "--upload-file-to-cache" "${APP_DIR}/${MODULE}.py" )
[ -n "${DEBUG:-}" ] && OPTS+=( "--log-level" "DEBUG" )

WD=$(mktemp -d)

if [ "$(hadoop_fs)" != "file" ]; then
    ensure_dfs_home
    INPUT="input"
    OUTPUT="output"
    ${HDFS} dfs -rm -r -f "${INPUT}" "${OUTPUT}"
    ${HDFS} dfs -put "${DATA}" "${INPUT}"
else
    INPUT="${DATA}"
    OUTPUT="${WD}/output"
fi
${PYDOOP} submit "${OPTS[@]}" ${MODULE} "${INPUT}" "${OUTPUT}"
${PYTHON} "${this_dir}"/check.py ${MODULE} "${OUTPUT}"

rm -rf "${WD}"
