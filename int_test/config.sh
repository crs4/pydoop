[ -n "${PYDOOP_INT_TESTS:-}" ] && return || readonly PYDOOP_INT_TESTS=1

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

ensure_dfs_home() {
    ${HDFS} dfs -mkdir -p /user/${USER}
}

export -f die ensure_dfs_home
