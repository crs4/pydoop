#!/usr/bin/env bash

set -euo pipefail

home="/opt/apache-maven"
repo="ftp://ftp.mirrorservice.org/sites/ftp.apache.org/maven/maven-3"

version=$(curl -l "${repo}"/ | sort | tail -n 1)
tar="apache-maven-${version}-bin.tar.gz"

curl -o ${tar} ${repo}/${version}/binaries/${tar}
tar xf ${tar}
mv apache-maven-${version} ${home}

cat >/etc/profile.d/maven.sh <<EOF
export M2_HOME="${home}"
export M2="\${M2_HOME}/bin"
export PATH="\${M2}:\${PATH}"
EOF
