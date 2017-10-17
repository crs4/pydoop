export USER="${USER:-$(whoami)}"
export HADOOP="${HADOOP:-hadoop}"
export PYTHON="${PYTHON:-python}"
export PY_VER=$("${PYTHON}" -c 'import sys; print(sys.version_info[0])')
export PYDOOP="pydoop${PY_VER}"
