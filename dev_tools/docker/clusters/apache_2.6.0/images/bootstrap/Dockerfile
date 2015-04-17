#----------------------------------------------------
FROM crs4_pydoop/apache_2.6.0_base:latest


COPY scripts/bootstrap.py /tmp/
COPY scripts/create_hdfs_dirs.sh /tmp/

CMD ["/usr/bin/python", "/tmp/bootstrap.py"]
    
