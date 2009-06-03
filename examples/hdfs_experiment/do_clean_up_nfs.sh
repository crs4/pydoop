#!/bin/bash
source .profile
conf_dir=$1
img_name=$2
h=`hostname`
img_name_specific=${img_name}.${h}
base=/scratchfs/shared/zag/images
cd ${base}

rm -vf /tmp/${img_name}*
rm -vf /scratchfs/shared/zag/images/${img_name}.*



