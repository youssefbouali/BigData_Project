from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml import PipelineModel
from kafka import KafkaProducer
import json

spark = SparkSession.builder \
    .appName("StreamingPredict") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .getOrCreate()

# Charger modèle
model = PipelineModel.load("/model/anomaly_model.spark")

# Lire Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "netflow-data") \
    .load()

# Convertir JSON
raw = df.selectExpr("CAST(value AS STRING) as json").select(from_json(col("json"), "src_ip STRING, dst_ip STRING, src_port INT, dst_port INT, protocol INT, bytes_in LONG, bytes_out LONG, pkt_in LONG, pkt_out LONG").alias("data")).select("data.*")

# Prédiction
predictions = model.transform(raw)

# Écrire dans Cassandra
query1 = predictions.select(
    monotonically_increasing_id().alias("id"),
    "src_ip", "prediction", "probability", current_timestamp().alias("timestamp")
).writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "security") \
    .option("table", "predictions") \
    .outputMode("append") \
    .start()

# Alerte Kafka si anomalie
def send_alert(row):
    producer = KafkaProducer(bootstrap_servers='kafka:9092')
    if row.prediction == 1:
        alert = {"ip": row.src_ip, "prob": row.probability[1], "time": str(row.timestamp)}
        producer.send("alerts", json.dumps(alert).encode('utf-8'))
    producer.close()

query2 = predictions.writeStream \
    .foreach(send_alert) \
    .start()

query1.awaitTermination()
query2.awaitTermination()