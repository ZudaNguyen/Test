FROM apache/spark:3.5.8-scala2.12-java17-python3-ubuntu@sha256:dbe5f3b3926b01fa0b818ae8463c2994bfd2945d8db96d2337e23dc62d6fa73d
USER root
COPY config/hadoop /opt/hadoop-conf
COPY config/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY src/verify_input.py /opt/project/src/verify_input.py
RUN mkdir -p /opt/project/logs/spark-events && chown -R 185:0 /opt/project/logs
USER 185
