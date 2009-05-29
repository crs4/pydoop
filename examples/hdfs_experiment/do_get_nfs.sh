#!/bin/bash
source .profile
conf_dir=$1
img_name=$2
h=`hostname`
trg=/tmp/${img_name}.${h}
cp /scratchfs/shared/zag/images/${img_name} ${trg}
#hdp="/ELS/els5/acdc/opt/hadoop-0.19.1/bin/hadoop --config ${conf_dir}"
#${hdp} fs -get ${img_name} ${trg}





