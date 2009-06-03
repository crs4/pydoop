#!/bin/bash
source .profile
conf_dir=$1
img_name=$2
h=`hostname`
trg=/tmp/${img_name}.${h}
base=/scratchfs/shared/zag/images
cd ${base}
cp ${base}/${img_name} ${trg}





