#!/bin/bash

# This script should be run in the client container, see initialize.sh

#-----------
function log() {
    echo "$1"
}


function install_hdp2_ubuntu_packages() {
    local VERSION="${1}"
    local HRTWRKS_REPO=http://public-repo-1.hortonworks.com/HDP/ubuntu12/2.x
    local HDP_LIST=${HRTWRKS_REPO}/GA/${VERSION}/hdp.list

    log "Adding repository"
    wget -nv ${HDP_LIST} -O /etc/apt/sources.list.d/hdp.list
    gpg --keyserver pgp.mit.edu --recv-keys B9733A7A07513CAD && gpg -a --export 07513CAD | apt-key add -
    apt-get update
    apt-get install -y hadoop hadoop-hdfs libhdfs0 \
                       hadoop-yarn hadoop-mapreduce hadoop-client \
                       openssl libsnappy1 libsnappy-dev
}


#----- Hadoop setup
hdp_ver=2.2.0.0
install_hdp2_ubuntu_packages ${hdp_ver}

export HADOOP_HOME=/usr/hdp/current/hadoop-client
export PATH=${HADOOP_HOME}/bin:${PATH}

#------------------
# Pydoop setup
git_url=https://github.com/crs4/pydoop.git

cat <<EOF > /home/aen/prepare_pydoop.sh
git clone ${git_url}
cd pydoop
python setup.py build
EOF

cat <<EOF > /home/aen/run_tests.sh
cd pydoop/test
python all_tests.py
EOF

cat <<EOF > /home/aen/run_examples.sh
cd pydoop/examples
./run_all
EOF

cat <<EOF > /home/aen/run_test_jar.sh
hdfs dfs -put run_test_jar.sh
yarn jar /usr/hdp/2.2.0.0-2041/hadoop-mapreduce/hadoop-mapreduce-examples.jar wordcount run_test_jar.sh foobar
EOF


#----------------------------------------------------
# Fix bad sw versions and missing things
apt-get install -y zip 
pip install setuptools --upgrade

#su - aen -c '/bin/bash ./prepare_pydoop.sh'
#cd /home/aen/pydoop
#python setup.py install
#cd
#su - aen -c '/bin/bash ./run_test_jar.sh'
#su - aen -c '/bin/bash ./run_tests.sh'
#su - aen -c '/bin/bash ./run_examples.sh'
