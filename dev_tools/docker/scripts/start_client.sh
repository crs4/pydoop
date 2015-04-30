#!/bin/bash

#-------------------------------------------
#
# Insert a new client in a running cluster
#
# Usage:
#        $ cd client_side_tests/<client>
#        $ ../../scripts/start_client.sh <PORT>
#
real_path=`readlink -f ${BASH_SOURCE[0]}`
script_dir=`dirname ${real_path}`
share_hosts_bin="python ${script_dir}/share_etc_hosts.py"

client_dir=`basename $PWD`
port=${1:-3333}

if [[ -z "${DOCKER_HOST_IP}" ]]
then 
	echo "No explicit DOCKER_HOST_IP in your env: localhost is assumed"
	DOCKER_HOST_IP=localhost
fi

# We assume that there is only one service with that name
cluster_tag=$(docker ps | grep resourcemanager | \
                     awk '{print $NF}'| sed -e 's/_.*$//')
client_name=${cluster_tag}_client_${client_dir}
docker run -d --name ${client_name} -p ${port}:22 crs4_pydoop/client:latest
${share_hosts_bin} ${cluster_tag}

rm_id=$(docker ps | grep resourcemanager | awk '{print $1}')
client_id=$(docker ps | grep ${client_name} | awk '{print $1}')

(cat ${HOME}/.ssh/id_dsa.pub | docker exec -i ${client_id} tee -a /root/.ssh/authorized_keys) > /dev/null

if [ -x ./initialize.sh ]; then
    ./initialize.sh ${port} ${client_id} ${rm_id}  ${DOCKER_HOST_IP}
fi


