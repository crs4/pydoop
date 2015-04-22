#!/bin/bash

cluster_name=$1
real_path=`realpath $0`
script_dir=`dirname ${real_path}`
share_hosts_bin="python ${script_dir}/share_etc_hosts.py"
cluster_path="${script_dir}/../clusters/${cluster_name}"

tag=`echo ${cluster_name} | tr -d '._/'`

cd ${cluster_path}

docker-compose stop
docker-compose rm

for x in logs local
do
    if [ -d ${x} ]; then
        backup=${x}.backup.$$
        mv ${x} ${backup}
        echo "Moved ${x} to ${backup}"
    fi
    mkdir ${x}
    chmod 1777 ${x}
done

docker-compose up -d
${share_hosts_bin} ${tag}
