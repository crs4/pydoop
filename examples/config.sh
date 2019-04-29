[ -n "${PYDOOP_EXAMPLES:-}" ] && return || readonly PYDOOP_EXAMPLES=1

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

hadoop_fs() {
    ${HDFS} getconf -confKey fs.defaultFS | cut -d : -f 1
}

export -f die ensure_dfs_home hadoop_fs
