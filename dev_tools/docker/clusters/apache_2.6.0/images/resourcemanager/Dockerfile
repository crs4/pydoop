#----------------------------------------------------
FROM crs4_pydoop/apache_2.6.0_base:latest

#
EXPOSE 8088 8021 8031 8032 8033

COPY scripts/start_resourcemanager.sh /tmp/

CMD ["/bin/bash", "/tmp/start_resourcemanager.sh"]

