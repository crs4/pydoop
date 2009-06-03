#!/bin/bash
source .profile
conf_dir=$1
img_name=$2
h=`hostname`
img_name_specific=${img_name}.${h}
trg=/tmp/${img_name_specific}.back
base=/scratchfs/shared/zag/images

cd ${base}
cp ${base}/${img_name_specific} ${trg}





