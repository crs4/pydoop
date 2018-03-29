#!/usr/bin/env bash

set -euo pipefail

VERSION="3.5.3"
TAR="apache-maven-${VERSION}-bin.tar.gz"
REPO="ftp://ftp.mirrorservice.org/sites/ftp.apache.org/maven/maven-3"
MVN_HOME="/opt/apache-maven"

curl -o ${TAR} ${REPO}/${VERSION}/binaries/${TAR}
tar xf ${TAR}
mv apache-maven-${VERSION} ${MVN_HOME}

cat >/etc/profile.d/maven.sh <<EOF
export M2_HOME="${MVN_HOME}"
export M2="\${M2_HOME}/bin"
export PATH="\${M2}:\${PATH}"
EOF
