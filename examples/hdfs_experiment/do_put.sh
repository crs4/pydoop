#!/bin/bash
source .profile
img_name=$1
config_dir=$2
h=`hostname`
trg=${img_name}.${h}

hdp="/ELS/els5/acdc/opt/hadoop-0.19.1/bin/hadoop --config ${config_dir}"
${hdp} fs -put /scratch/${trg} ${trg}




