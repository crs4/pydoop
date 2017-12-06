#!/usr/bin/env bash

# BEGIN_COPYRIGHT
#
# Copyright 2009-2017 CRS4.
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

nargs=1
if [ $# -ne ${nargs} ]; then
    die "Usage: $0 module_name"
fi
MODULE=$1

JOBNAME=${MODULE}
INPUT=${MODULE}_input
OUTPUT=${MODULE}_output
RESULTS=results.txt

OPTS=(
    "-D" "mapreduce.task.timeout=10000"
    "-D" "mapred.map.tasks=2"
    "--python-program" "${PYTHON}"
    "--job-name" "${JOBNAME}"
)
case ${MODULE} in
    wordcount_minimal )
	DATA="${this_dir}"/../input/alice.txt
	APP_DIR="${this_dir}/../wordcount/bin"
	OPTS+=("--entry-point" "main")
	;;
    wordcount_full )
	DATA="${this_dir}"/../input/alice.txt
	APP_DIR="${this_dir}/../wordcount/bin"
	OPTS+=("--entry-point" "main")
	OPTS+=( "--do-not-use-java-record-reader" )
	OPTS+=( "--do-not-use-java-record-writer" )
	OPTS+=( "-D" "pydoop.hdfs.user=${USER}" )
	;;
    nosep )
	DATA="${this_dir}/data/cols.txt"
	APP_DIR="${this_dir}/mr"
	OPTS+=( "--num-reducers" "0" )
	OPTS+=( "--output-format" "it.crs4.pydoop.NoSeparatorTextOutputFormat" )
	;;
    map_only_java_writer )
	DATA="${this_dir}"/../input/alice.txt
	APP_DIR="${this_dir}/mr"
	OPTS+=( "--num-reducers" "0" )
	;;
    map_only_python_writer )
	DATA="${this_dir}"/../input/alice.txt
	APP_DIR="${this_dir}/mr"
	OPTS+=( "--num-reducers" "0" )
	OPTS+=( "--do-not-use-java-record-writer" )
	;;
esac
OPTS+=( "--upload-file-to-cache" "${APP_DIR}/${MODULE}.py" )
[ -n "${DEBUG:-}" ] && OPTS+=( "--log-level" "DEBUG" )

${HADOOP} fs -rmr "/user/${USER}/${INPUT}" || :
${HADOOP} fs -mkdir -p "/user/${USER}/${INPUT}"
${HADOOP} fs -rmr "/user/${USER}/${OUTPUT}" || :
${HADOOP} fs -put "${DATA}" "${INPUT}"
${PYDOOP} submit "${OPTS[@]}" ${MODULE} "${INPUT}" "${OUTPUT}"
${PYTHON} "${this_dir}"/check.py ${MODULE} "${OUTPUT}"
