#!/usr/bin/env bash

# BEGIN_COPYRIGHT
#
# Copyright 2009-2024 CRS4.
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

pushd "${this_dir}"

[ $# -ge 1 ] || die "Usage: $0 APP_NAME"
name=$1

${PYTHON} -c "import pydoop; pydoop.check_local_mode()"

opts=(
    "-D" "mapreduce.job.name=${name}"
    "-D" "mapreduce.task.timeout=10000"
)

wd=$(mktemp -d)

case ${name} in
    map_only_java_writer )
	input="input/map_only"
	opts+=(
	    "-D" "mapreduce.pipes.isjavarecordreader=true"
	    "-D" "mapreduce.pipes.isjavarecordwriter=true"
	    "-reduces" "0"
	)
	;;
    map_only_python_writer )
	input="input/map_only"
	opts+=(
	    "-D" "mapreduce.pipes.isjavarecordreader=true"
	    "-D" "mapreduce.pipes.isjavarecordwriter=false"
	    "-reduces" "0"
	)
	;;
    map_reduce_combiner )
	io_sort_mb=1
	file_size=$((2 * io_sort_mb * 1024 * 1024))
	input="${wd}/map_reduce_very_long"
	${PYTHON} genwords.py "${input}" --file-size ${file_size}
	opts+=(
	    "-D" "mapreduce.pipes.isjavarecordreader=true"
	    "-D" "mapreduce.pipes.isjavarecordwriter=true"
	    "-D" "mapreduce.task.io.sort.mb=${io_sort_mb}"
	    "-reduces" "2"
	)
	;;
    map_reduce_python_reader )
	input="input/map_reduce"
	opts+=(
	    "-D" "mapreduce.pipes.isjavarecordreader=false"
	    "-D" "mapreduce.pipes.isjavarecordwriter=true"
	    "-reduces" "2"
	)
	;;
    map_reduce_python_writer )
	input="input/map_reduce"
	opts+=(
	    "-D" "mapreduce.pipes.isjavarecordreader=true"
	    "-D" "mapreduce.pipes.isjavarecordwriter=false"
	    "-reduces" "2"
	)
	;;
    map_reduce_slow_java_rw )
	input="input/map_reduce_long"
	opts+=(
	    "-D" "mapreduce.job.maps=1"
	    "-D" "mapreduce.pipes.isjavarecordreader=true"
	    "-D" "mapreduce.pipes.isjavarecordwriter=true"
	    "-reduces" "1"
	)
	;;
    map_reduce_slow_python_rw )
	input="input/map_reduce_long"
	opts+=(
	    "-D" "mapreduce.job.maps=1"
	    "-D" "mapreduce.pipes.isjavarecordreader=false"
	    "-D" "mapreduce.pipes.isjavarecordwriter=false"
	    "-reduces" "1"
	)
	;;
    map_reduce_* )
	input="input/map_reduce"
	opts+=(
	    "-D" "mapreduce.pipes.isjavarecordreader=true"
	    "-D" "mapreduce.pipes.isjavarecordwriter=true"
	    "-reduces" "2"
	)
	;;
    * )
	rm -rf "${wd}"
	die "unknown app name: \"${name}\""
esac

mrapp="mr/${name}.py"
[ -e "${mrapp}" ] || die "\"${mrapp}\" not found"

# wrap the python app with a bash layer that sets PATH
cat >"${wd}/mrapp" <<EOF
#!${BASH}
""":"
export PATH="${PATH}"
exec "${PYTHON}" -u "\$0" "\$@"
":"""
EOF
cat "${mrapp}" >>"${wd}/mrapp"
mrapp="${wd}/mrapp"

ensure_dfs_home
${HDFS} dfs -rm -r -f "input" "output" "mrapp.py" "pstats"
${HDFS} dfs -put "${input}" "input"
${HDFS} dfs -put "${mrapp}" "mrapp.py"
${MAPRED} pipes "${opts[@]}" -program "mrapp.py" -input "input" -output "output"

echo "checking results"
${HDFS} dfs -get output "${wd}/output"
case "${name}" in
    *pstats )
	${HDFS} dfs -get pstats "${wd}/output.stats"
	;;
esac
${PYTHON} check.py "${name}" "${input}" "${wd}/output"

rm -rf "${wd}"
popd
