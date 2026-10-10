# Flink 1.20 (the LTS line) plus the two connectors the Tideline jobs need.
# The SQL is checked against Flink's planner in CI (pipeline/flink/check_sql.py).
FROM flink:1.20.3-scala_2.12-java17

ARG MAVEN=https://repo1.maven.org/maven2/org/apache/flink
RUN wget -q -P /opt/flink/lib \
      "$MAVEN/flink-sql-connector-kafka/3.4.0-1.20/flink-sql-connector-kafka-3.4.0-1.20.jar" \
      "$MAVEN/flink-sql-avro-confluent-registry/1.20.3/flink-sql-avro-confluent-registry-1.20.3.jar" \
 && chown flink:flink /opt/flink/lib/*.jar \
 && mkdir -p /flink-checkpoints && chown flink:flink /flink-checkpoints

COPY sql /opt/tideline/sql
