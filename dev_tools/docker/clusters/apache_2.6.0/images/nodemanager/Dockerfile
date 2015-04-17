#----------------------------------------------------
FROM crs4_pydoop/apache_2.6.0_base:latest

#
EXPOSE 8042

COPY scripts/start_nodemanager.sh /tmp/

CMD ["/bin/bash", "/tmp/start_nodemanager.sh"]

