#!/bin/bash
source .profile
conf_dir=$1
img_name=$2
h=`hostname`
img_name_specific=${img_name}.${h}
trg=/var/${img_name_specific}.back
cp /scratchfs/shared/zag/images/${img_name_specific} ${trg}
#hdp="/ELS/els5/acdc/opt/hadoop-0.19.1/bin/hadoop --config ${conf_dir}"
#${hdp} fs -get ${img_name_specific} ${trg}
#ls -l ${trg}




