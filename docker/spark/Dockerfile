FROM bitnami/spark:latest

USER root
RUN apt-get update && apt-get install -y python3-pip && pip3 install --upgrade pip

COPY requirements.txt .
COPY scripts ./scripts
RUN pip3 install -r requirements.txt

# Cassandra & Kafka connectors - use latest compatible versions
# Get Spark version dynamically or use latest stable connector versions
RUN wget -q https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.4.0/spark-cassandra-connector-assembly_2.12-3.4.0.jar -P /opt/bitnami/spark/jars/ || \
    wget -q https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.3.0/spark-cassandra-connector-assembly_2.12-3.3.0.jar -P /opt/bitnami/spark/jars/
RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.4.0/spark-sql-kafka-0-10_2.12-3.4.0.jar -P /opt/bitnami/spark/jars/ || \
    wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar -P /opt/bitnami/spark/jars/

COPY start-master.sh /start-master.sh
COPY start-worker.sh /start-worker.sh
RUN chmod +x /start-master.sh /start-worker.sh

CMD ["bash"]
