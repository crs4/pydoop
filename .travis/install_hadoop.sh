#!/bin/bash

set -o errexit
set -o nounset

HadoopArchiveUrl="http://archive.apache.org/dist/hadoop/core/"
TravisHadoopEnvFile="/tmp/set_travis_hadoop_env.sh"

# a generic error trap that prints the command that failed before exiting the script.
function error_trap() {
  printf -v message "Unexpected error while installing Hadoop.\nCommand: %s\nExiting\n" "${BASH_COMMAND}"
  printf "${message}" >&2
  exit 1
}

trap error_trap ERR

function log() {
  echo -e $(date +"%F %T") -- $@ >&2
  return 0
}

function error() {
    if [ -n "${@}" ]; then
        log $@
    else
        log "Unknown error"
    fi
    exit 1
}


function write_old_style_site_config() {
    [ $# -eq 1 ] || error "Missing Hadoop conf dir function argument"
    local HadoopConfDir="${1}"

    cat <<END > "${HadoopConfDir}/hdfs-site.xml"
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property><name>dfs.permissions.supergroup</name><value>admin</value></property>
    <property><name>dfs.replication</name><value>1</value></property>
    <property><name>dfs.namenode.fs-limits.min-block-size</name><value>512</value></property>
    <property><name>dfs.namenode.secondary.http-address</name><value>localhost:50090</value></property>
</configuration>
END
    cat <<END > "${HadoopConfDir}/core-site.xml"
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://localhost:9000</value>
    </property>
</configuration>
END

    cat <<END > "${HadoopConfDir}/mapred-site.xml"
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapred.job.tracker</name>
        <value>localhost:9001</value>
    </property>
</configuration>
END
    return 0
}

function write_yarn_site_config() {
    [ $# -eq 1 ] || error "Missing Hadoop conf dir function argument"
    local HadoopConfDir="${1}"

    sudo cat <<END > "${HadoopConfDir}/core-site.xml"
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.default.name</name>
        <value>hdfs://localhost:8020</value>
    </property>

    <!-- OOZIE proxy user setting -->
    <property><name>hadoop.proxyuser.oozie.hosts</name><value>*</value></property>
    <property><name>hadoop.proxyuser.oozie.groups</name><value>*</value></property>

    <!-- HTTPFS proxy user setting -->
    <property><name>hadoop.proxyuser.httpfs.hosts</name><value>*</value></property>
    <property><name>hadoop.proxyuser.httpfs.groups</name><value>*</value></property>
</configuration>
END

        #sed "s/localhost /localhost `hostname` /" /etc/hosts > /tmp/hosts; sudo mv /tmp/hosts /etc/hosts
        #sudo /etc/init.d/networking restart
    sudo cat <<END > "${HadoopConfDir}/mapred-site.xml"
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>mapred.job.tracker</name>
        <value>localhost:9001</value>
    </property>
    <property>
        <name>mapred.local.dir</name>
        <value>/tmp/mapred_data</value>
    </property>
</configuration>
END

    sudo cat <<END > "${HadoopConfDir}/hdfs-site.xml"
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property><name>dfs.permissions.supergroup</name><value>admin</value></property>
    <property><name>dfs.replication</name><value>1</value></property>
    <property><name>dfs.namenode.secondary.http-address</name><value>localhost:50090</value></property>
</configuration>
END
    return 0
}

function install_standard_hadoop() {
    [ $# -eq 1 ] || error "Missing HadoopVersion function argument"
    local HadoopVersion="${1}"

    log "Installing standard Apache Hadoop, version ${HadoopVersion}"

    wget ${HadoopArchiveUrl}/hadoop-${HadoopVersion}/hadoop-${HadoopVersion}.tar.gz
    tar xzf "hadoop-${HadoopVersion}.tar.gz"

    export HADOOP_HOME="${PWD}/hadoop-${HadoopVersion}"
    if [[ "${HadoopVersion}" == 2.2.* || "${HadoopVersion}" == 2.4.* ]]; then
        export HADOOP_CONF_DIR="${PWD}/.travis/hadoop-${HadoopVersion}-conf/"
        export HADOOP_BIN="${HADOOP_HOME}/sbin/"
        export HADOOP_COMMON_LIB_NATIVE_DIR="${HADOOP_HOME}/lib/native"
        export HADOOP_OPTS="-Djava.library.path=${HADOOP_HOME}/lib"
    else 
        export HADOOP_CONF_DIR="${HADOOP_HOME}/conf"
        export HADOOP_BIN="${HADOOP_HOME}/bin/"
        write_old_style_site_config "${HADOOP_CONF_DIR}"
    fi
    echo "export HADOOP_HOME=${HADOOP_HOME}" >> "${HADOOP_CONF_DIR}/hadoop-env.sh"
    echo "export JAVA_HOME=${JAVA_HOME}" >> "${HADOOP_CONF_DIR}/hadoop-env.sh"
    # copy the path from the current environment (which may have been modified
    # in .travis.yml steps prior to this one).
    echo "export PATH=${PATH}" >> "${HADOOP_CONF_DIR}/hadoop-env.sh"
    
    log "Formatting namenode"
    "${HADOOP_HOME}/bin/hadoop" namenode -format
    log "Starting daemons..."
    "${HADOOP_BIN}/start-all.sh"
    "${HADOOP_HOME}/bin/hadoop" dfsadmin -safemode wait
    log "done"
    return 0
}


function install_cdh() {
    [ $# -eq 1 ] || error "Missing HadoopVersion function argument"
    local HadoopVersion="${1}"

    log "Installing Cloudera Hadoop, version ${HadoopVersion}"

    log "Creating namenode directories"
    mkdir -p /tmp/hadoop-hdfs/dfs/name
    ls -ld /tmp/hadoop-hdfs/dfs/name
    chmod 777 /tmp/hadoop-hdfs/ -R
    ls -la /tmp/hadoop-hdfs/dfs/name

    log "Installing packages"
    if [[ "${HadoopVersion}" == *cdh4* ]]; then 
        sudo add-apt-repository "deb [arch=amd64] http://archive.cloudera.com/cdh4/ubuntu/precise/amd64/cdh precise-${HadoopVersion} contrib"
        curl -s http://archive.cloudera.com/cdh4/ubuntu/lucid/amd64/cdh/archive.key | sudo apt-key add -
        sudo apt-get update
        if [[ "$YARN" ]]; then
            sudo apt-get install hadoop-conf-pseudo hadoop-0.20-mapreduce-jobtracker hadoop-client hadoop-0.20-mapreduce-tasktracker
        else
            sudo apt-get install hadoop-0.20-mapreduce-jobtracker hadoop-hdfs-datanode hadoop-hdfs-namenode hadoop-hdfs-secondarynamenode hadoop-client hadoop-0.20-mapreduce-tasktracker
        fi
    elif [[ "${HadoopVersion}" == *cdh3* ]]; then
        sudo add-apt-repository "deb [arch=amd64] http://archive.cloudera.com/debian lucid-${HadoopVersion} contrib"
        curl -s http://archive.cloudera.com/debian/archive.key | sudo apt-key add -
        sudo apt-get update
        sudo apt-get install hadoop-0.20-conf-pseudo
    fi

    local HadoopConfDir=/etc/hadoop/conf/
    log "Creating configuration under ${HadoopConfDir}"
    # make configuration files editable by everyone to simplify setting up the machine... :-/
    sudo chmod -R 777 "${HadoopConfDir}"
    sed -i -e '/\/configuration/ i\<property><name>dfs.permissions.supergroup<\/name><value>admin<\/value><\/property>' "${HadoopConfDir}/hdfs-site.xml"
    if [[ "${YARN}" ]]; then                
        write_yarn_site_config "${HadoopConfDir}"

        #sudo rm /tmp/hadoop* -rf
        #sudo rm /var/lib/hadoop-hdfs/* -rf
        sudo mkdir /tmp/mapred_data
        sudo chown -R mapred:hadoop /tmp/mapred_data
    fi

    for i in `cd /etc/init.d; ls hadoop*`; do sudo service $i stop; done   
    
    #if [[ "${HadoopVersion}" == *cdh3* ]]
    #   then 
    #      JH=${JAVA_HOME//\//\\\/}
    #     sed "s/# export JAVA_HOME=.*/ export JAVA_HOME=${JH//\//\\\/}/" /etc/hadoop/conf/hadoop-env.sh > /tmp/env.sh; sudo mv /tmp/env.sh /etc/hadoop/conf/hadoop-env.sh
    #fi
    
    echo "export JAVA_HOME=$JAVA_HOME" >> "${HadoopConfDir}/hadoop-env.sh"
    # copy the path from the current environment (which may have been modified
    # in .travis.yml steps prior to this one).
    echo "export PATH=${PATH}" >> "${HadoopConfDir}/hadoop-env.sh"

    log "Formatting namenode"
    sudo rm /tmp/hadoop-hdfs/dfs/name -rf
    sudo -u hdfs hadoop namenode -format

    log "Starting namenode"
    for x in `cd /etc/init.d ; ls hadoop-*namenode` ; do sudo service $x start ; done

    hadoop dfsadmin -safemode wait
    log "HDFS out of safe mode"
    sudo jps

    local hdfs="sudo -u hdfs hadoop fs"
    log "Creating HDFS directories"
    ${hdfs} -mkdir /tmp
    ${hdfs} -chmod 1777 /tmp
    ${hdfs} -mkdir  /user/$USER
    ${hdfs} -chown $USER /user/$USER
    
     if [[ "$YARN" ]]; then
        ${hdfs}  -mkdir /tmp/hadoop-yarn/staging
        ${hdfs}  -chmod -R 1777 /tmp/hadoop-yarn/staging
        
        ${hdfs} -mkdir /tmp/hadoop-yarn/staging/history/done_intermediate
        ${hdfs} -chmod -R 1777 /tmp/hadoop-yarn/staging/history/done_intermediate
        ${hdfs} -chown -R mapred:mapred /tmp/hadoop-yarn/staging
        ${hdfs} -mkdir /var/log/hadoop-yarn 
        ${hdfs} -chown yarn:mapred /var/log/hadoop-yarn
        
        sudo service hadoop-yarn-resourcemanager start
        sudo service hadoop-yarn-nodemanager start 
        sudo service hadoop-mapreduce-historyserver start
        export HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce
    else
        ${hdfs} -mkdir /var/lib/hadoop-hdfs/cache/mapred/mapred/staging
        ${hdfs} -chmod 1777 /var/lib/hadoop-hdfs/cache/mapred/mapred/staging
        ${hdfs} -chown -R mapred /var/lib/hadoop-hdfs/cache/mapred
        
        ${hdfs} -mkdir /tmp/mapred/system
        ${hdfs} -chown mapred:hadoop /tmp/mapred/system
    fi        

    log "Starting all Hadoop services"
    for x in `cd /etc/init.d ; ls hadoop-*` ; do sudo service $x start ; done

    log "done"

    return 0
}

function print_hadoop_env() {
    for var_name in HADOOP_HOME\
               HADOOP_CONF_DIR\
               HADOOP_BIN\
               HADOOP_COMMON_LIB_NATIVE_DIR\
               HADOOP_OPTS\
               HADOOP_CONF_DIR\
               HADOOP_BIN\
               HADOOP_MAPRED_HOME ;
    do
        # derefence the variable
        if [[ -v ${var_name} ]]; then
            value=$(eval echo \$${var_name})
            printf "export ${var_name}=\"${value}\"\n"
        fi
    done
}

#### main ###

if [[ "${HADOOPVERSION}" != *cdh* ]]; then
    install_standard_hadoop "${HADOOPVERSION}"
else # else CDH
    install_cdh "${HADOOPVERSION}"
fi

print_hadoop_env > "${TravisHadoopEnvFile}"
chmod a+r "${TravisHadoopEnvFile}"
log "Wrote hadoop environment variables to ${TravisHadoopEnvFile}\n   ==== Start ===="
cat ${TravisHadoopEnvFile} >&2
log "   ====  End  ===="

log "installation finished"

# turn off verification of variables
# The Travis build process crashes otherwise
set +o nounset
