#!/bin/bash

port=$1
client_id=$2
rm_container_id=$3
DOCKER_HOST_IP=${4:-localhost}
#----------------------------------
client_name=`docker exec ${client_id} hostname`

#----------------------------------
scp -P${port} local_client_setup.sh root@${DOCKER_HOST_IP}:.

# exec and remove the installer script
ssh -p${port} root@${DOCKER_HOST_IP} './local_client_setup.sh && rm local_client_setup.sh'

# copy the hadoop configuration from the resourcemanager container to the client container
echo "Copying hadoop config from the resourcemanager container..."
for c in core-site.xml mapred-site.xml yarn-site.xml
do
    from=/opt/hadoop/etc/hadoop/${c}
    to=/etc/hadoop/conf/${c}
    docker exec -it ${rm_container_id} scp ${from} ${client_name}:${to}
done

