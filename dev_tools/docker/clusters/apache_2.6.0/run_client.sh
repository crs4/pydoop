#!/bin/bash

#-----------
# This script should be run in the client container.

#----- Hadoop setup
hdp_ver=hadoop-2.6.0
hdp_tgz=${hdp_ver}.tar.gz
hdp_url=http://mirror.nohup.it/apache/hadoop/common/${hdp_ver}/${hdp_tgz}
pushd /opt
wget ${hdp_url} -O ${hdp_tgz}
tar xzf ${hdp_tgz}
ln -s ./${hdp_ver} hadoop
cat <<EOF  > /opt/hadoop/etc/hadoop/core-site.xml
<configuration>
<property>
<name>fs.defaultFS</name>
<value>hdfs://namenode:9000</value>
</property>
</configuration>
EOF

cat <<EOF  > /opt/hadoop/etc/hadoop/yarn-site.xml
<configuration>
	<property>
	  <name>yarn.resourcemanager.hostname</name>
	  <value>resourcemanager</value>
	</property>
</configuration>
EOF
export HADOOP_HOME=/opt/hadoop
export PATH=${HADOOP_HOME}/bin:${PATH}
popd

#------------------
# Pydoop setup
git_url=https://github.com/crs4/pydoop.git

cat <<EOF > /home/aen/prepare_pydoop.sh
export HADOOP_HOME=/opt/hadoop
git clone ${git_url}
cd pydoop
python setup.py build
EOF

cat <<EOF > /home/aen/run_tests.sh
export HADOOP_HOME=/opt/hadoop
export PATH=\${HADOOP_HOME}/bin:\${PATH}
cd pydoop/test
python all_tests.py
EOF

cat <<EOF > /home/aen/run_examples.sh
export HADOOP_HOME=/opt/hadoop
export PATH=\${HADOOP_HOME}/bin:\${PATH}
cd pydoop/examples
./run_all
EOF

cat <<EOF > /home/aen/run_test_jar.sh
export HADOOP_HOME=/opt/hadoop
export PATH=\${HADOOP_HOME}/bin:\${PATH}
hdfs dfs -put run_test_jar.sh
yarn jar /opt/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-*.jar wordcount run_test_jar.sh foobar
EOF


#----------------------------------------------------
# Fix bad sw versions
pip install setuptools --upgrade

#su - aen -c '/bin/bash ./prepare_pydoop.sh'
#su - aen -c '/bin/bash ./run_test_jar.sh'
#su - aen -c '/bin/bash ./run_tests.sh'
#su - aen -c '/bin/bash ./run_examples.sh'




