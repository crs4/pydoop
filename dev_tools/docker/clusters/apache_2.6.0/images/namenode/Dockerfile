#----------------------------------------------------
FROM crs4_pydoop/apache_2.6.0_base:latest

# HDFS WebUI and HDFS default port
EXPOSE  50070 9000

COPY scripts/start_namenode.sh /tmp/

CMD ["/bin/bash", "/tmp/start_namenode.sh"]
    
