#!/bin/bash

set -euo pipefail
[ -n "${DEBUG:-}" ] && set -x

function onshutdown {
    mr-jobhistory-daemon.sh stop historyserver
    yarn-daemon.sh stop nodemanager
    yarn-daemon.sh stop resourcemanager
}

trap onshutdown SIGTERM
trap onshutdown SIGINT

conf_dir=$(dirname $(dirname $(command -v hadoop)))/etc/hadoop
cat >"${conf_dir}"/core-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
</configuration>
EOF
cat >"${conf_dir}"/hdfs-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
</configuration>
EOF

yarn-daemon.sh start resourcemanager
yarn-daemon.sh start nodemanager
mr-jobhistory-daemon.sh start historyserver

tail -f /dev/null

onshutdown
