FROM eclipse-temurin:17-jre-alpine
RUN apk add --no-cache bash wget
RUN wget -q https://archive.apache.org/dist/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && \
    tar -xzf spark-3.5.1-bin-hadoop3.tgz && \
    mv spark-3.5.1-bin-hadoop3 /opt/spark && \
    rm spark-3.5.1-bin-hadoop3.tgz
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
COPY target/spark-streaming-1.0-SNAPSHOT.jar /opt/spark/app.jar
COPY entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh
ENTRYPOINT ["/opt/spark/entrypoint.sh"]
