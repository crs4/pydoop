#!/bin/bash -x
source ~/.profile
echo $PATH

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
do_clean_up=${base}/do_clean_up.sh
hod="/ELS/els5/acdc/svn/hod_ge/releases/REL-0.1/bin/hod  -c /u/zag/zag_hod.sge"
hdp="/ELS/els5/acdc/opt/hadoop-0.19.1/bin/hadoop --config ${conf_dir}"

echo "Running experiment with ${N} nodes." >> ${log_file}

${hod} -t /u/acdc/hadoop/hadoop-0.19.1.tar.gz  -o "allocate ${conf_dir} ${N}"

python ${base}/generate_files.py ${conf_dir} ${node_subsets}

${hdp} fs -put ${img_file}  ${img_name}
for f in ${node_subsets}/*;
do
    dsh -f ${f} -cM --  rm -f /scratch/${img_name}\*
    T0=`date +%s`
    dsh -f ${f} -cM --  ${do_get}  ${conf_dir} ${img_name}
    T1=`date +%s`
    dsh -f ${f} -cM --  ${do_put}  ${conf_dir} ${img_name}
    T2=`date +%s`
    dsh -f ${f} -cM --  ${do_get_specific}  ${conf_dir} ${img_name}
    T3=`date +%s`
    dsh -f ${f} -cM --  ${do_clean_up}  ${conf_dir} ${img_name}
    T4=`date +%s`
    echo "${f} $T0 $T1 $T2 $T3 $T4" >> ${log_file}
done

${hod} deallocate -d ${conf_dir}


