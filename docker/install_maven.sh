#!/usr/bin/env bash

set -euo pipefail

repo="ftp://ftp.mirrorservice.org/sites/ftp.apache.org/maven/maven-3"
dist="apache-maven-${maven_version}"
tar="${dist}-bin.tar.gz"

wget -q -O - ${repo}/${maven_version}/binaries/${tar} | tar xz
mv ${dist} ${maven_home}

cat >/etc/profile.d/maven.sh <<EOF
export M2_HOME="${maven_home}"
export M2="\${M2_HOME}/bin"
export PATH="\${M2}:\${PATH}"
EOF
