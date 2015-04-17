#!/bin/bash

real_path=`realpath $0`
script_dir=`dirname ${real_path}`


CLUSTER=$1
TAG=`basename ${CLUSTER}`

SHARE_HOSTS="python ${script_dir}/share_etc_hosts.py"
echo ${CLUSTER}  ${TAG} ${SHARE_HOSTS}

docker-compose -f ${CLUSTER}/docker-compose.yml up -d
${SHARE_HOSTS} ${TAG}
