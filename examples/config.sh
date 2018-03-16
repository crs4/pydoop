[ -n "${PYDOOP_EXAMPLES:-}" ] && return || readonly PYDOOP_EXAMPLES=1

build_parquet_jar() {
    pushd $1
    sbt assembly
    popd
}

die() {
    echo $1 1>&2
    exit 1
}

export USER="${USER:-$(whoami)}"
export HADOOP="${HADOOP:-hadoop}"
export HDFS="${HDFS:-hdfs}"
export MAPRED="${MAPRED:-mapred}"
export YARN="${YARN:-yarn}"
export PYTHON="${PYTHON:-python}"
export PY_VER=$("${PYTHON}" -c 'import sys; print(sys.version_info[0])')
export PYDOOP="pydoop${PY_VER}"

export -f build_parquet_jar die
