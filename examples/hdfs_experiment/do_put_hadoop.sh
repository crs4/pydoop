#!/bin/bash
source .profile
config_dir=$1
img_name=$2
h=`hostname`
trg=${img_name}.${h}

hdp="/ELS/els5/acdc/opt/hadoop-0.19.1/bin/hadoop --config ${config_dir}"
${hdp} fs -put /tmp/${trg} ${trg}





