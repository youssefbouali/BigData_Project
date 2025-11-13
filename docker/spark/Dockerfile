FROM eclipse-temurin:11-jre

USER root
ENV SPARK_VERSION=3.3.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:${SPARK_HOME}/bin:${SPARK_HOME}/sbin
ENV PYSPARK_PYTHON=python3

# Install dependencies
RUN apt-get update && apt-get install -y \
    wget \
    curl \
    python3 \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Skip pip upgrade - pip 24.0 from apt is recent enough
# If upgrade needed later, use: python3 -m ensurepip --upgrade

# Download and install Spark
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install Python dependencies
COPY requirements.txt .
COPY scripts ./scripts
RUN pip3 install --break-system-packages -r requirements.txt

# Cassandra & Kafka connectors for Spark 3.3.0
RUN wget -q https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector-assembly_2.12/3.3.0/spark-cassandra-connector-assembly_2.12-3.3.0.jar -P ${SPARK_HOME}/jars/
RUN wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.3.0/spark-sql-kafka-0-10_2.12-3.3.0.jar -P ${SPARK_HOME}/jars/

COPY docker/spark/start-master.sh /start-master.sh
COPY docker/spark/start-worker.sh /start-worker.sh
RUN chmod +x /start-master.sh /start-worker.sh

CMD ["bash"]
