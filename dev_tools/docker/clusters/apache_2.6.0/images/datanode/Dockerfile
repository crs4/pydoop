#----------------------------------------------------
FROM crs4_pydoop/apache_2.6.0_base:latest

#
EXPOSE  50020

COPY scripts/start_datanode.sh /tmp/

CMD ["/bin/bash", "/tmp/start_datanode.sh"]

