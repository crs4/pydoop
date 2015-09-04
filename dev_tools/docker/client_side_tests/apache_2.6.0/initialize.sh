#!/bin/bash

port=$1
client_id=$2
rm_container_id=$3
DOCKER_HOST_IP=${4:-localhost}
#----------------------------------
client_name=`docker exec ${client_id} hostname`

#----- Upload hadoop to the client container
hdp_ver=hadoop-2.6.0
hdp_tgz=${hdp_ver}.tar.gz
if [[ ! -f ${hdp_tgz} ]]
then
	hdp_url=http://mirror.nohup.it/apache/hadoop/common/${hdp_ver}/${hdp_tgz}
	wget ${hdp_url} -O ${hdp_tgz}
fi

# copy the hadoop*.tar.gz
scp -P${port} ${hdp_tgz} root@${DOCKER_HOST_IP}:/opt/

# copy the installer script
scp -P${port} local_client_setup.sh root@${DOCKER_HOST_IP}:.

# exec and remove the installer script
ssh -p${port} root@${DOCKER_HOST_IP} './local_client_setup.sh && rm local_client_setup.sh'

# copy the hadoop configuration from the resourcemanager container to the client container
echo "Copying hadoop config from the resourcemanager container..."
for c in core-site.xml mapred-site.xml yarn-site.xml
do
    from=/opt/hadoop/etc/hadoop/${c}
    to=/opt/hadoop/etc/hadoop/${c}
    docker exec -it ${rm_container_id} scp ${from} ${client_name}:${to}
done

