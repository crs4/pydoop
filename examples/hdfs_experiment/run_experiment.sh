#!/bin/bash -x
N=$1
base=/ELS/els5/zag
log_file=${base}/log_$$_${N}.log
img_name=fake.img
img_file=/ELS/els5/acdc/images/${img_name}
conf_dir=${base}/hod_entu_${N}
node_subsets=${base}/node_subsets_${N}

do_get=${base}/do_get.sh
do_put=${base}/do_put.sh
do_get_specific=${base}/do_get_specific.sh
hod="/ELS/els5/acdc/svn/hod_ge/releases/REL-0.1/bin/hod  -c ~/zag_hod.sge"
hdp="/ELS/els5/acdc/opt/hadoop-0.19.1/bin/hadoop --config ${conf_dir}"


echo "Running experiment with ${N} nodes." >> ${log_file}

${hod} -t /u/acdc/hadoop/hadoop-0.19.1.tar.gz  -o "allocate ${conf_dir} ${N}"

python ${base}/generate_files.py ${conf_dir} ${node_subsets}

${hdp}  -put ${img_file}  ${img_file}
for f in ${node_subsets}/*;
do
    T0=`date +%s`
    dsh -f ${f} -cM --  ${do_get}  ${conf_dir} ${img_name}
    T1=`date +%s`
    dsh -f ${f} -cM --  ${do_put}  ${conf_dir} ${img_name}
    T2=`date +%s`
    dsh -f ${f} -cM --  ${do_get_specific}  ${conf_dir} ${img_name}
    T3=`date +%s`
    echo "${f} $T0 $T1 $T2 $T3" >> ${log_file}
done




