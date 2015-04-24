#!/bin/bash

#-----------
#

if [[ -z "${DOCKER_HOST_IP}" ]]
then 
	echo "No explicit DOCKER_HOST_IP in your env: localhost is assumed"
	DOCKER_HOST_IP=localhost
fi

# register the key
cat "${HOME}/.ssh/id_dsa.pub" | ssh -p2222 root@${DOCKER_HOST_IP} 'mkdir -p ~/.ssh && cat >> ~/.ssh/authorized_keys'

# copy the installer script
scp -P2222 local_client_setup.sh root@${DOCKER_HOST_IP}:.

# exect and remove the installer script
ssh -p2222 root@${DOCKER_HOST_IP} './local_client_setup.sh && rm local_client_setup.sh'

# copy the hadoop configuration from the resourcemanager container to the client container
echo "Copying hadoop config from the resourcemanager container..."
rm_container_id=$(docker ps | grep resourcemanager | awk '{print $1}')

for c in core-site.xml mapred-site.xml yarn-site.xml
do
    from=/opt/hadoop/etc/hadoop/${c}
    to=/etc/hadoop/conf/${c}
    docker exec -it ${rm_container_id} scp ${from} client:${to}
done

