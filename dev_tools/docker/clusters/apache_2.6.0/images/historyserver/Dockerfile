#----------------------------------------------------
FROM crs4_pydoop/apache_2.6.0_base:latest

#
EXPOSE 10020 19888

COPY scripts/start_historyserver.sh /tmp/

CMD ["/bin/bash", "/tmp/start_historyserver.sh"]

