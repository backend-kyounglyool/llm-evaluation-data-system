# Flink 세션 클러스터 이미지: Kafka SQL 커넥터 + S3 파일시스템 플러그인(s3:// raw 적재용)
FROM flink:1.20.2-scala_2.12-java17

RUN wget -q -P /opt/flink/lib \
      https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar \
 && mkdir -p /opt/flink/plugins/s3-fs-hadoop \
 && cp /opt/flink/opt/flink-s3-fs-hadoop-*.jar /opt/flink/plugins/s3-fs-hadoop/ \
 && mkdir -p /opt/flink/checkpoints \
 && chown -R flink:flink /opt/flink/lib /opt/flink/plugins /opt/flink/checkpoints
