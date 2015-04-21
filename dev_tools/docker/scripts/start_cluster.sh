#!/bin/bash

cluster_name=$1
real_path=`realpath $0`
script_dir=`dirname ${real_path}`
share_hosts_bin="python ${script_dir}/share_etc_hosts.py"
cluster_path="${script_dir}/../clusters/${cluster_name}"

tag=`echo ${cluster_name} | tr -d '._/'`

cd ${cluster_path}
docker-compose up -d
${share_hosts_bin} ${tag}
