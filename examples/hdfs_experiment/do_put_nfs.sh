#!/bin/bash
source .profile
config_dir=$1
img_name=$2
h=`hostname`
trg=${img_name}.${h}

cp /var/${trg} /scratchfs/shared/zag/images/${trg}

#hdp="/ELS/els5/acdc/opt/hadoop-0.19.1/bin/hadoop --config ${config_dir}"
#${hdp} fs -put /scratch/${trg} ${trg}





