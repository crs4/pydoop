#!/bin/bash
set -e

if [[ "$1" != *cdh* ]]; #standard hadoop distribution
    then
        wget http://archive.apache.org/dist/hadoop/core/hadoop-$1/hadoop-$1.tar.gz
        tar xf hadoop-$1.tar.gz
        export HADOOP_HOME=`pwd`/hadoop-$1; 
        if [[ "$1" == 2.2.* ]];
            then
                export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop; 
                export HADOOP_BIN=$HADOOP_HOME/sbin; 
                export HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_HOME}/lib/native; 
                export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib";
                echo "<?xml version=\"1.0\"?> <configuration> <property> <name>yarn.resourcemanager.resource-tracker.address</name> <value>localhost:9050</value> <description>host is the hostname of the resource manager and port is the port on which the NodeManagers contact the Resource Manager. </description> </property>  <property> <name>yarn.resourcemanager.scheduler.address</name> <value>localhost:9051</value> <description>host is the hostname of the resourcemanager and port is the port on which the Applications in the cluster talk to the Resource Manager. </description> </property>  <property> <name>yarn.resourcemanager.scheduler.class</name> <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value> <description>In case you do not want to use the default scheduler</description> </property>  <property> <value>localhost:9052</value> <name>yarn.resourcemanager.address</name> <description>the host is the hostname of the ResourceManager and the port is the port on which the clients can talk to the Resource Manager. </description> </property>  <property> <name>yarn.nodemanager.local-dirs</name> <value>/tmp/nodemanager</value> <description>the local directories used by the nodemanager</description> </property>  <property> <name>yarn.nodemanager.address</name> <value>localhost:9053</value> <description>the nodemanagers bind to this port</description> </property>  <property> <name>yarn.nodemanager.resource.memory-mb</name> <value>10240</value> <description>the amount of memory on the NodeManager in GB</description> </property>  <property> <name>yarn.nodemanager.remote-app-log-dir</name> <value>/tmp/app-logs</value> <description>directory on hdfs where the application logs are moved to </description> </property> <property> <name>yarn.nodemanager.aux-services</name> <value>mapreduce_shuffle</value> <description>shuffle service that needs to be set for Map Reduce to run </description> </property> </configuration>" > $HADOOP_CONF_DIR/yarn-site.xml; 
            else 
                export HADOOP_CONF_DIR=$HADOOP_HOME/conf; 
                export HADOOP_BIN=$HADOOP_HOME/bin;
                
            fi
        echo "<?xml version=\"1.0\"?><?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?><configuration><property><name>dfs.permissions.supergroup</name><value>admin</value></property><property><name>dfs.replication</name><value>1</value></property><property><name>dfs.namenode.fs-limits.min-block-size</name><value>512</value></property><property><name>dfs.namenode.secondary.http-address</name><value>localhost:50090</value></property></configuration>" > $HADOOP_CONF_DIR/hdfs-site.xml;
        echo "<?xml version=\"1.0\"?><?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?><configuration><property><name>fs.default.name</name><value>hdfs://localhost:9000</value></property></configuration>" > $HADOOP_CONF_DIR/core-site.xml;
        echo "<?xml version=\"1.0\"?><?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?><configuration> <property><name>mapred.job.tracker</name><value>localhost:9001</value></property></configuration>" > $HADOOP_CONF_DIR/mapred-site.xml;
        $HADOOP_HOME/bin/hadoop namenode -format;
        $HADOOP_BIN/start-all.sh; 
        $HADOOP_HOME/bin/hadoop dfsadmin -safemode wait;

    else
        mkdir -p /tmp/hadoop-hdfs/dfs/name
        chmod 777 /tmp/hadoop-hdfs/ -R
        ls -la /tmp/hadoop-hdfs/dfs/name;
        if [[ "$1" == *cdh4* ]]; 
            then 
                sudo add-apt-repository "deb [arch=amd64] http://archive.cloudera.com/cdh4/ubuntu/precise/amd64/cdh precise-$1 contrib";  curl -s http://archive.cloudera.com/cdh4/ubuntu/lucid/amd64/cdh/archive.key | sudo apt-key add -; 
                sudo apt-get update; sudo apt-get install hadoop-0.20-mapreduce-jobtracker hadoop-hdfs-datanode hadoop-hdfs-namenode hadoop-hdfs-secondarynamenode hadoop-client;
        else if [[ "$1" == *cdh3* ]];
            then
                sudo add-apt-repository "deb [arch=amd64] http://archive.cloudera.com/debian lucid-$1 contrib";  curl -s http://archive.cloudera.com/debian/archive.key | sudo apt-key add -;
                sudo apt-get update; sudo apt-get install hadoop-0.20-conf-pseudo;
            fi
        fi
        
        sudo chmod 666 /etc/hadoop/conf/*.xml
        sudo echo "<?xml version=\"1.0\"?><?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?><configuration><property><name>fs.default.name</name><value>hdfs://localhost:8020</value></property><!-- OOZIE proxy user setting --><property><name>hadoop.proxyuser.oozie.hosts</name><value>*</value></property><property><name>hadoop.proxyuser.oozie.groups</name><value>*</value></property><!-- HTTPFS proxy user setting --><property><name>hadoop.proxyuser.httpfs.hosts</name><value>*</value></property><property><name>hadoop.proxyuser.httpfs.groups</name><value>*</value></property></configuration>" > /etc/hadoop/conf/core-site.xml
        sudo sed '/\/configuration/ i\<property><name>dfs.permissions.supergroup<\/name><value>admin<\/value><\/property>' <  /etc/hadoop/conf/hdfs-site.xml > /tmp/hdfs-site.xml; sudo mv /tmp/hdfs-site.xml /etc/hadoop/conf/hdfs-site.xml
        sed "s/localhost /localhost `hostname` /" /etc/hosts > /tmp/hosts; sudo mv /tmp/hosts /etc/hosts
        sudo /etc/init.d/networking restart
        sudo -u hdfs hadoop namenode -format -force
        if [[ "$1" == *cdh4* ]];
            then 
            sudo service hadoop-hdfs-datanode restart; sudo service hadoop-hdfs-namenode restart; 
        else if [[ "$1" == *cdh3* ]];
            then 
                JH=${JAVA_HOME//\//\\\/};
                sed "s/# export JAVA_HOME=.*/ export JAVA_HOME=${JH//\//\\\/}/" /etc/hadoop/conf/hadoop-env.sh > /tmp/env.sh; sudo mv /tmp/env.sh /etc/hadoop/conf/hadoop-env.sh; 
                sudo service hadoop-0.20-datanode restart; sudo service hadoop-0.20-namenode restart
            fi    
        fi
        hadoop dfsadmin -safemode wait;
        hdfs="sudo -u hdfs hadoop fs"; 
        ${hdfs} -mkdir /tmp; 
        ${hdfs} -chmod 1777 /tmp; 
        ${hdfs} -mkdir /var/lib/hadoop-hdfs/cache/mapred/mapred/staging; 
        ${hdfs} -chmod 1777 /var/lib/hadoop-hdfs/cache/mapred/mapred/staging;
        ${hdfs} -chown -R mapred /var/lib/hadoop-hdfs/cache/mapred;
        ${hdfs} -mkdir  /user/$USER;
        ${hdfs} -chown $USER /user/$USER;        
fi
