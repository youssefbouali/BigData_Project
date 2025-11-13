
FROM bitnami/spark:3
USER root
RUN apt-get update && apt-get install -y python3 python3-pip default-jdk-headless && rm -rf /var/lib/apt/lists/*
RUN pip3 install pyspark==3.3.2 kafka-python cassandra-driver elasticsearch
WORKDIR /app
COPY run_spark.sh /app/run_spark.sh
COPY spark_job.py /app/spark_job.py
RUN chmod +x /app/run_spark.sh
ENTRYPOINT ["/app/run_spark.sh"]
