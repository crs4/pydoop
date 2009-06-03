#!/bin/bash
source .profile
config_dir=$1
img_name=$2
h=`hostname`
trg=${img_name}.${h}
base=/scratchfs/shared/zag/images

cd ${base}
cp /tmp/${trg} ${base}/${trg}







