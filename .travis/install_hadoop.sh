#!/bin/bash
#set -e

if [[ "$HADOOPVERSION" != *cdh* ]]; #standard hadoop distribution
    then
        #wget http://archive.apache.org/dist/hadoop/core/hadoop-$HADOOPVERSION/hadoop-$HADOOPVERSION.tar.gz
        tar xf hadoop-$HADOOPVERSION.tar.gz
        export HADOOP_HOME=`pwd`/hadoop-$HADOOPVERSION; 
        if [[ "$HADOOPVERSION" == 2.2.* ]];
            then
                export HADOOP_CONF_DIR=`pwd`/.travis/hadoop-$HADOOPVERSION-conf/; 
                export HADOOP_BIN=$HADOOP_HOME/sbin; 
                export HADOOP_COMMON_LIB_NATIVE_DIR=${HADOOP_HOME}/lib/native; 
                export HADOOP_OPTS="-Djava.library.path=$HADOOP_HOME/lib";
            else 
                export HADOOP_CONF_DIR=$HADOOP_HOME/conf; 
                export HADOOP_BIN=$HADOOP_HOME/bin;

		echo "<?xml version=\"1.0\"?><?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?><configuration><property><name>dfs.permissions.supergroup</name><value>admin</value></property><property><name>dfs.replication</name><value>1</value></property><property><name>dfs.namenode.fs-limits.min-block-size</name><value>512</value></property><property><name>dfs.namenode.secondary.http-address</name><value>localhost:50090</value></property></configuration>" > $HADOOP_CONF_DIR/hdfs-site.xml;
	        echo "<?xml version=\"1.0\"?><?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?><configuration><property><name>fs.default.name</name><value>hdfs://localhost:9000</value></property></configuration>" > $HADOOP_CONF_DIR/core-site.xml;
	        echo "<?xml version=\"1.0\"?><?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?><configuration> <property><name>mapred.job.tracker</name><value>localhost:9001</value></property></configuration>" > $HADOOP_CONF_DIR/mapred-site.xml;

                
            fi
        echo "export HADOOP_HOME=$HADOOP_HOME" >> $HADOOP_CONF_DIR/hadoop-env.sh
	echo "export JAVA_HOME=$JAVA_HOME" >> $HADOOP_CONF_DIR/hadoop-env.sh
        
	$HADOOP_HOME/bin/hadoop namenode -format;
        $HADOOP_BIN/start-all.sh; 
        $HADOOP_HOME/bin/hadoop dfsadmin -safemode wait;

    else
        mkdir -p /tmp/hadoop-hdfs/dfs/name
        chmod 777 /tmp/hadoop-hdfs/ -R
        ls -la /tmp/hadoop-hdfs/dfs/name;
        if [[ "$HADOOPVERSION" == *cdh4* ]]; 
            then 
                sudo add-apt-repository "deb [arch=amd64] http://archive.cloudera.com/cdh4/ubuntu/precise/amd64/cdh precise-$HADOOPVERSION contrib";  curl -s http://archive.cloudera.com/cdh4/ubuntu/lucid/amd64/cdh/archive.key | sudo apt-key add -; 
                sudo apt-get update; 
                if [[ "$YARN" ]];
					then sudo apt-get install hadoop-conf-pseudo;
				else
					sudo apt-get install hadoop-0.20-mapreduce-jobtracker hadoop-hdfs-datanode hadoop-hdfs-namenode hadoop-hdfs-secondarynamenode hadoop-client hadoop-0.20-mapreduce-tasktracker;
                fi
                
        else if [[ "$HADOOPVERSION" == *cdh3* ]];
            then
                sudo add-apt-repository "deb [arch=amd64] http://archive.cloudera.com/debian lucid-$HADOOPVERSION contrib";  curl -s http://archive.cloudera.com/debian/archive.key | sudo apt-key add -;
                sudo apt-get update; sudo apt-get install hadoop-0.20-conf-pseudo;
            fi
        fi
        
        
        if ! [[ "$YARN" ]];
			then				
				sudo chmod 666 /etc/hadoop/conf/*.xml
				sudo echo "<?xml version=\"1.0\"?><?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?><configuration><property><name>fs.default.name</name><value>hdfs://localhost:8020</value></property><!-- OOZIE proxy user setting --><property><name>hadoop.proxyuser.oozie.hosts</name><value>*</value></property><property><name>hadoop.proxyuser.oozie.groups</name><value>*</value></property><!-- HTTPFS proxy user setting --><property><name>hadoop.proxyuser.httpfs.hosts</name><value>*</value></property><property><name>hadoop.proxyuser.httpfs.groups</name><value>*</value></property></configuration>" > /etc/hadoop/conf/core-site.xml
				sudo sed '/\/configuration/ i\<property><name>dfs.permissions.supergroup<\/name><value>admin<\/value><\/property>' <  /etc/hadoop/conf/hdfs-site.xml > /tmp/hdfs-site.xml; sudo mv /tmp/hdfs-site.xml /etc/hadoop/conf/hdfs-site.xml
				sed "s/localhost /localhost `hostname` /" /etc/hosts > /tmp/hosts; sudo mv /tmp/hosts /etc/hosts
				sudo /etc/init.d/networking restart
				sudo echo "<?xml version=\"1.0\"?><?xml-stylesheet type=\"text/xsl\" href=\"configuration.xsl\"?><configuration><property><name>mapred.job.tracker</name><value>localhost:9001</value></property><property><name>mapred.local.dir</name><value>/tmp/mapred_data</value></property></configuration>" > /etc/hadoop/conf/mapred-site.xml;
				#sudo rm /tmp/hadoop* -rf
				#sudo rm /var/lib/hadoop-hdfs/* -rf
				sudo mkdir /tmp/mapred_data
				sudo chown -R mapred:hadoop /tmp/mapred_data
		
		fi
		
		for i in `cd /etc/init.d; ls hadoop*`; do sudo service $i stop; done   
        
        if [[ "$HADOOPVERSION" == *cdh3* ]];
            then 
                JH=${JAVA_HOME//\//\\\/};
                sed "s/# export JAVA_HOME=.*/ export JAVA_HOME=${JH//\//\\\/}/" /etc/hadoop/conf/hadoop-env.sh > /tmp/env.sh; sudo mv /tmp/env.sh /etc/hadoop/conf/hadoop-env.sh; 
        fi
        
        sudo -u hdfs hadoop namenode -format -force
        
        for x in `cd /etc/init.d ; ls hadoop-hdfs-*` ; do sudo service $x start ; done
        hadoop dfsadmin -safemode wait;
        hdfs="sudo -u hdfs hadoop fs"; 
        ${hdfs} -mkdir /tmp; 
        ${hdfs} -chmod 1777 /tmp; 
        ${hdfs} -mkdir  /user/$USER;
        ${hdfs} -chown $USER /user/$USER;
        
         if [[ "$YARN" ]];
			then
			${hdfs}  -mkdir /tmp/hadoop-yarn/staging
			${hdfs}  -chmod -R 1777 /tmp/hadoop-yarn/staging
			
			${hdfs} -mkdir /tmp/hadoop-yarn/staging/history/done_intermediate;
			${hdfs} -chmod -R 1777 /tmp/hadoop-yarn/staging/history/done_intermediate
			${hdfs} -chown -R mapred:mapred /tmp/hadoop-yarn/staging
			${hdfs} -mkdir /var/log/hadoop-yarn 
			${hdfs} -chown yarn:mapred /var/log/hadoop-yarn
			
			sudo service hadoop-yarn-resourcemanager start
			sudo service hadoop-yarn-nodemanager start 
			sudo service hadoop-mapreduce-historyserver start
			export HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce
			
		else
		
			${hdfs} -mkdir /var/lib/hadoop-hdfs/cache/mapred/mapred/staging; 
			${hdfs} -chmod 1777 /var/lib/hadoop-hdfs/cache/mapred/mapred/staging;
			${hdfs} -chown -R mapred /var/lib/hadoop-hdfs/cache/mapred;
			
			${hdfs} -mkdir /tmp/mapred/system
			${hdfs} -chown mapred:hadoop /tmp/mapred/system
			
			sudo jps
			cat /var/log/hadoop-0.20-mapreduce/hadoop-hadoop-jobtracker-*.log
			sudo service hadoop-0.20-mapreduce-jobtracker restart;
			sudo service hadoop-0.20-mapreduce-tasktracker restart;
			sudo jps
		fi        
        
fi
