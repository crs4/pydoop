#!/bin/bash


if [[ -z "${DOCKER_HOST_IP}" ]]
then 
	echo "No explicit DOCKER_HOST_IP in your env: localhost is assumed"
	DOCKER_HOST_IP=localhost
fi

cl_container_id=$(docker ps | grep client | awk '{print $1}')
rm_container_id=$(docker ps | grep resourcemanager | awk '{print $1}')

(cat ${HOME}/.ssh/id_dsa.pub | docker exec -i ${cl_container_id} tee -a /root/.ssh/authorized_keys) > /dev/null

scp -P2222 local_client_setup.sh root@${DOCKER_HOST_IP}:.

# exec and remove the installer script
ssh -p2222 root@${DOCKER_HOST_IP} './local_client_setup.sh && rm local_client_setup.sh'

# copy the hadoop configuration from the resourcemanager container to the client container
echo "Copying hadoop config from the resourcemanager container..."
for c in core-site.xml mapred-site.xml yarn-site.xml
do
    from=/opt/hadoop/etc/hadoop/${c}
    to=/etc/hadoop/conf/${c}
    docker exec -it ${rm_container_id} scp ${from} client:${to}
done

