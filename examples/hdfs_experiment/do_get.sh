#!/bin/bash
source .profile
conf_dir=$1
img_name=$2
h=`hostname`
trg=/scratch/${img_name}.${h}
hdp="/ELS/els5/acdc/opt/hadoop-0.19.1/bin/hadoop --config ${conf_dir}"
${hdp} fs -get ${img_name} ${trg}
#ls -l ${trg}




