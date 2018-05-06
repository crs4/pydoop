#!/usr/bin/env bash

set -euo pipefail
[ -n "${DEBUG:-}" ] && set -x
this="${BASH_SOURCE-$0}"
this_dir=$(cd -P -- "$(dirname -- "${this}")" && pwd -P)
. "${this_dir}/../config.sh"
. "${this_dir}/config.sh"

pushd "${this_dir}"
gen_classpath
cp="$(<"${CP_PATH}"):$(${HADOOP} classpath)"
mkdir -p "${CLASS_DIR}"
javac -cp "${cp}" -d "${CLASS_DIR}" src/main/java/it/crs4/pydoop/*
jar -cf "${JAR_PATH}" -C "${CLASS_DIR}" ./it
popd
