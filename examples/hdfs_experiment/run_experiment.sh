#!/bin/bash -x
source ~/.profile
echo $PATH

exp_type=$1
N=$2
exp_id=${exp_type}-${N}

echo "Running experiment of type ${exp_type} with a ${N} nodes hdfs cluster."
echo "experiment id = ${exp_id}"


base=/ELS/els5/zag
log_file=${base}/log_$$_${exp_id}.log
img_name=fake.img
img_file=/ELS/els5/acdc/images/${img_name}
conf_dir=${base}/hod_entu_${exp_id}_$$
node_subsets=${base}/node_subsets_${exp_id}_$$


case ${exp_type} in
    HDFS_1)
	hod_conf=${base}/hod_replication=1.sge
	do_get=${base}/do_get_hadoop.sh
	do_put=${base}/do_put_hadoop.sh
	do_get_specific=${base}/do_get_specific_hadoop.sh
	do_clean_up=${base}/do_clean_up_hadoop.sh
	;;
    HDFS_2)
	hod_conf=${base}/hod_replication=2.sge
	do_get=${base}/do_get_hadoop.sh
	do_put=${base}/do_put_hadoop.sh
	do_get_specific=${base}/do_get_specific_hadoop.sh
	do_clean_up=${base}/do_clean_up_hadoop.sh
	;;
    HDFS_3)
	hod_conf=${base}/hod_replication=3.sge
	do_get=${base}/do_get_hadoop.sh
	do_put=${base}/do_put_hadoop.sh
	do_get_specific=${base}/do_get_specific_hadoop.sh
	do_clean_up=${base}/do_clean_up_hadoop.sh
	;;
    NFS)
	hod_conf=${base}/hod_nfs.sge
	do_get=${base}/do_get_nfs.sh
	do_put=${base}/do_put_nfs.sh
	do_get_specific=${base}/do_get_specific_nfs.sh
	do_clean_up=${base}/do_clean_up_nfs.sh
	;;
esac

(cat  <<EOF 
Running experiment of type ${exp_type} with a ${N} nodes hdfs cluster.
experiment id = ${exp_id}
Using:
   conf_dir=${conf_dir}
   node_subsets=${node_subsets}
   hod_conf=${hod_conf}
   do_get=${do_get}
   do_put=${do_put}
   do_get_specific=${do_get_specific}
   do_clean_up=${do_clean_up}
EOF
) > ${log_file}


# we run the hadoop cluster anyway...
hod="/ELS/els5/acdc/svn/hod_ge/releases/REL-0.1.1/bin/hod  -c ${hod_conf}"
#hod="/ELS/els5/acdc/svn/hod_ge/branches/0.1/bin/hod  -c ${hod_conf}"
hdp="/ELS/els5/acdc/opt/hadoop-0.19.1/bin/hadoop --config ${conf_dir}"
${hod} -t /u/acdc/hadoop/hadoop-0.19.1.tar.gz  -o "allocate ${conf_dir} ${N}"

if [ ${exp_type} == "NFS" ]; then
    pushd /scratchfs/shared/zag/images
    popd
    cp ${img_file} /scratchfs/shared/zag/images/
else
    sleep 120
    ${hdp} fs -put ${img_file}  ${img_name}
fi

python ${base}/generate_files.py ${conf_dir} ${node_subsets}

for f in ${node_subsets}/*;
do
    dsh -f ${f} -cM --  rm -f /tmp/${img_name}\*
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


