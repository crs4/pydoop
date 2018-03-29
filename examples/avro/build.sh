#!/usr/bin/env bash

set -euo pipefail
[ -n "${DEBUG:-}" ] && set -x
this="${BASH_SOURCE-$0}"
this_dir=$(cd -P -- "$(dirname -- "${this}")" && pwd -P)
. "${this_dir}/../config.sh"

nargs=2
if [ $# -ne ${nargs} ]; then
    die "Usage: $0 CP_PATH JAR_PATH"
fi
CP_PATH=$1
JAR_PATH=$2

WD=$(mktemp -d)
CLASS_DIR="${WD}"/classes
mkdir -p "${CLASS_DIR}"

pushd "${this_dir}"
./dep_classpath "${CP_PATH}"
cp="$(<"${CP_PATH}"):$(${HADOOP} classpath)"
javac -cp "${cp}" -d "${CLASS_DIR}" java/src/main/java/it/crs4/pydoop/*
jar -cf "${JAR_PATH}" -C "${CLASS_DIR}" ./it
popd

rm -rf "${WD}"
