#----------------------------------------------------
FROM crs4_pydoop/base:latest

# ------------------------------------------------------------------
# Get zookeeper
     
ENV zoo_ver zookeeper-3.4.6
ENV zoo_tgz ${zoo_ver}.tar.gz
ENV zoo_site http://mirror.nohup.it/apache/zookeeper
ENV zoo_tgz_site ${zoo_site}/${zoo_ver}

RUN wget ${zoo_tgz_site}/${zoo_tgz} -O ${zoo_tgz} && \
    mkdir -p /opt && tar -C /opt -xzf ${zoo_tgz} && rm -f ${zoo_tgz} && \
    ln -s /opt/${zoo_ver} /opt/zookeeper

ENV ZOO_DATA_DIR      /data/zookeeper/data
ENV ZOO_CLIENT_PORT   2181

EXPOSE ${ZOO_CLIENT_PORT}

RUN mkdir -p ${ZOO_DATA_DIR}
RUN echo "tickTime=2000"                  > /opt/zookeeper/conf/zoo.cfg && \
    echo "dataDir ${ZOO_DATA_DIR}"       >> /opt/zookeeper/conf/zoo.cfg && \
    echo "clientPort ${ZOO_CLIENT_PORT}" >> /opt/zookeeper/conf/zoo.cfg && \
    echo 1 > ${ZOO_DATA_DIR}/myid

# Note that we are forcing the installation to dist-packages,
# so that it will be possible to share kazoo and externally mount /usr/local later.
RUN pip install kazoo -t /usr/lib/python2.7/dist-packages
COPY scripts/zk_wait.py /tmp/
COPY scripts/zk_set.py /tmp/
# -----------------------------------------------------------------
# Get hadoop

ENV hdp_ver hadoop-2.6.0
ENV hdp_tgz ${hdp_ver}.tar.gz
ENV hdp_site http://mirror.nohup.it/apache/hadoop/common
ENV hdp_tgz_site ${hdp_site}/hadoop-2.6.0

RUN wget ${hdp_tgz_site}/${hdp_tgz} -O ${hdp_tgz} && \
    mkdir -p /opt && tar -C /opt -xzf ${hdp_tgz} && rm -f ${hdp_tgz} && \
    ln -s /opt/${hdp_ver} /opt/hadoop

# ------------------------------------------------------------------
# User:Group	   Daemons
# hdfs:hadoop	   NameNode, Secondary NameNode, JournalNode, DataNode
# yarn:hadoop	   ResourceManager, NodeManager
# mapred:hadoop	 MapReduce JobHistory Server

ENV HADOOP_GROUP hadoop
ENV HDFS_USER hdfs
ENV YARN_USER yarn
ENV MAPRED_USER mapred

ENV HDP_DATA_ROOT /data/hadoop
ENV LOG_DIR ${HDP_DATA_ROOT}/log

ENV DFS_NAME_DIR ${HDP_DATA_ROOT}/hdfs/nn
ENV DFS_DATA_DIR ${HDP_DATA_ROOT}/hdfs/dn
ENV DFS_CHECKPOINT_DIR   ${HDP_DATA_ROOT}/hdfs/snn
ENV HDFS_LOG_DIR ${LOG_DIR}/hdfs
ENV HDFS_PID_DIR ${HDP_DATA_ROOT}/pid/hdfs

ENV YARN_LOCAL_DIR ${HDP_DATA_ROOT}/yarn
ENV YARN_LOG_DIR ${HDP_DATA_ROOT}/yarn/logs
ENV YARN_LOCAL_LOG_DIR ${YARN_LOCAL_DIR}/userlogs

ENV YARN_PID_DIR ${HDP_DATA_ROOT}/pid/yarn
ENV MAPRED_LOG_DIR   ${LOG_DIR}/mapred
ENV MAPRED_PID_DIR   ${HDP_DATA_ROOT}/pid/mapred
ENV HADOOP_CONF_DIR ${HDP_DATA_ROOT}/etc/conf

ENV YARN_REMOTE_APP_LOG_DIR   /app-logs
ENV MAPRED_JH_INTERMEDIATE_DONE_DIR /mr-history/tmp
ENV MAPRED_JH_DONE_DIR /mr-history/done

#----------------------------------------------------------

RUN groupadd ${HADOOP_GROUP} && \
    useradd -g ${HADOOP_GROUP} ${HDFS_USER} && \
    useradd -g ${HADOOP_GROUP} ${YARN_USER} && \
    useradd -g ${HADOOP_GROUP} ${MAPRED_USER}

RUN mkdir -p ${HDP_DATA_ROOT} && \
    chmod -R 755 ${HDP_DATA_ROOT} && \
    mkdir -p ${LOG_DIR} && \
    chmod -R 1777 ${LOG_DIR} && \
    mkdir -p ${HADOOP_CONF_DIR} && \
    mkdir -p ${DFS_NAME_DIR} && \
    chown -R ${HDFS_USER}:${HADOOP_GROUP} ${DFS_NAME_DIR} && \
    chmod -R 755 ${DFS_NAME_DIR} && \
    mkdir -p ${DFS_DATA_DIR} && \
    chown -R ${HDFS_USER}:${HADOOP_GROUP} ${DFS_DATA_DIR} && \
    chmod -R 750 ${DFS_DATA_DIR} && \
    mkdir -p ${DFS_CHECKPOINT_DIR} && \
    chown -R ${HDFS_USER}:${HADOOP_GROUP} ${DFS_CHECKPOINT_DIR} && \
    chmod -R 750 ${DFS_CHECKPOINT_DIR} && \
    mkdir -p ${HDFS_LOG_DIR} && \
    chown -R ${HDFS_USER}:${HADOOP_GROUP} ${HDFS_LOG_DIR} && \
    chmod -R 750 ${HDFS_LOG_DIR} && \
    mkdir -p ${HDFS_PID_DIR} && \
    chown -R ${HDFS_USER}:${HADOOP_GROUP} ${HDFS_PID_DIR} && \
    chmod -R 750 ${HDFS_PID_DIR}
 
RUN mkdir -p ${YARN_LOCAL_DIR} && \
    chown -R ${YARN_USER}:${HADOOP_GROUP} ${YARN_LOCAL_DIR} && \
    chmod -R 755 ${YARN_LOCAL_DIR}

RUN mkdir -p ${YARN_LOG_DIR} && \
    chown -R $YARN_USER:$HADOOP_GROUP $YARN_LOG_DIR && \
    chmod -R 755 $YARN_LOG_DIR && \
    mkdir -p $YARN_LOCAL_LOG_DIR && \
    chown -R $YARN_USER:$HADOOP_GROUP $YARN_LOCAL_LOG_DIR && \
    chmod -R 755 $YARN_LOCAL_LOG_DIR && \
    mkdir -p $YARN_PID_DIR && \
    chown -R $YARN_USER:$HADOOP_GROUP $YARN_PID_DIR && \
    chmod -R 755 $YARN_PID_DIR && \
    mkdir -p $MAPRED_LOG_DIR && \
    chown -R $MAPRED_USER:$HADOOP_GROUP $MAPRED_LOG_DIR && \
    chmod -R 755 $MAPRED_LOG_DIR && \
    mkdir -p $MAPRED_PID_DIR && \
    chown -R $MAPRED_USER:$HADOOP_GROUP $MAPRED_PID_DIR && \
    chmod -R 755 $MAPRED_PID_DIR

RUN mkdir -p $ $YARN_REMOTE_APP_LOG_DIR && \
    chown -R $YARN_USER:$HADOOP_GROUP $YARN_REMOTE_APP_LOG_DIR && \
    chmod -R 777 $YARN_REMOTE_APP_LOG_DIR


COPY scripts/generate_conf_files.py /tmp/
RUN python2.7 /tmp/generate_conf_files.py ${HADOOP_CONF_DIR}

ENV HADOOP_HOME /opt/hadoop
ENV PATH ${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:$PATH

RUN mkdir /tmp/logs && chmod 1777 /tmp/logs