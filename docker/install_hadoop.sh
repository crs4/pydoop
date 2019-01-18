#!/usr/bin/env bash

set -euo pipefail

tar="hadoop-${HADOOP_VERSION}.tar.gz"
repo="http://www-eu.apache.org/dist/hadoop/common"

mkdir -p "${HADOOP_HOME}"
wd=$(mktemp -d)
pushd "${wd}"
curl ${repo}/hadoop-${HADOOP_VERSION}/${tar} | tar xz
mv hadoop-${HADOOP_VERSION}/* "${HADOOP_HOME}"
popd
rm -rf "${wd}"
