from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import uuid

spark = SparkSession.builder \
    .appName("NetFlowPipeline") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "netflow-data") \
    .load()

logs = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), "src_ip STRING, dest_ip STRING, protocol STRING, bytes_in BIGINT, bytes_out BIGINT").alias("data")) \
    .select("data.*")

query = logs.withColumn("id", expr("uuid()")) \
    .writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .options(table="flows", keyspace="netflow") \
    .outputMode("append") \
    .start()

query.awaitTermination()
