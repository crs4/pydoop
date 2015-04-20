#!/bin/bash

real_path=`realpath $0`
cluster_path=$1

#----------------
script_dir=`dirname ${real_path}`
share_hosts_bin="python ${script_dir}/share_etc_hosts.py"

cluster_dir=`basename ${cluster_path}`
tag=`echo ${cluster_dir} | tr -d '._/'`


cd ${cluster_path}
docker-compose up -d
${share_hosts_bin} ${tag}
