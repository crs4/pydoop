#!/bin/bash

#-----------
#

if [[ -z "${DOCKER_HOST_IP}" ]]
then 
	echo "No explicit DOCKER_HOST_IP in your env: localhost is assumed"
	DOCKER_HOST_IP=localhost
fi

#----- Upload hadoop to the client container
hdp_ver=hadoop-2.6.0
hdp_tgz=${hdp_ver}.tar.gz
if [[ ! -f ${hdp_tgz} ]]
then
	hdp_url=http://mirror.nohup.it/apache/hadoop/common/${hdp_ver}/${hdp_tgz}
	wget ${hdp_url} -O ${hdp_tgz}
fi

# register the key
cat "${HOME}/.ssh/id_dsa.pub" | ssh -p2222 root@${DOCKER_HOST_IP} 'mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys'

# copy the hadoop*.tar.gz
scp -P2222 ${hdp_tgz} root@${DOCKER_HOST_IP}:/opt/

# copy the installer script
scp -P2222 local_client_setup.sh root@${DOCKER_HOST_IP}:.

# exect and remove the installer script
ssh -p2222 root@${DOCKER_HOST_IP} './local_client_setup.sh && rm local_client_setup.sh'

# copy the hadoop configuration from the resourcemanager container to the client container
echo "Copying hadoop config from the resourcemanager container..."
rm_container_id=$(docker ps | grep resourcemanager | awk '{print $1}')
docker exec -it ${rm_container_id} scp -r /opt/hadoop/etc/hadoop client:/opt/hadoop/etc

