FROM apache/hadoop:3.4.1@sha256:69ffa97339aff768c4e6120c3fb27aa04c121402b1c8158408a5fb5be586a30e
USER root
COPY config/hadoop/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
COPY config/hadoop/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
COPY scripts/hdfs-entrypoint.sh scripts/import-hdfs.sh /opt/project/
RUN mkdir -p /opt/project/data/raw
