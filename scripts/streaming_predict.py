from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml import PipelineModel
from kafka import KafkaProducer
import json
from datetime import datetime

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

# Extract probability as double for Cassandra (probability is a DenseVector)
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType

def extract_prob_value(prob):
    try:
        if prob is not None:
            # DenseVector has .values array
            if hasattr(prob, 'values') and len(prob.values) > 1:
                return float(prob.values[1])
            elif hasattr(prob, '__getitem__'):
                return float(prob[1])
        return 0.0
    except:
        return 0.0

extract_prob_udf = udf(extract_prob_value, DoubleType())

# Prepare data for Cassandra
predictions_with_prob = predictions.withColumn("prob_value", extract_prob_udf("probability"))

# Écrire dans Cassandra
query1 = predictions_with_prob.select(
    monotonically_increasing_id().alias("id"),
    "src_ip",
    "prediction",
    col("prob_value").alias("probability"),
    current_timestamp().alias("timestamp")
).writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "security") \
    .option("table", "predictions") \
    .outputMode("append") \
    .start()

# Alerte Kafka si anomalie
kafka_producer = None

def send_alert_batch(batch_df, batch_id):
    global kafka_producer
    if kafka_producer is None:
        kafka_producer = KafkaProducer(
            bootstrap_servers='kafka:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
    
    anomalies = batch_df.filter(batch_df.prediction == 1).collect()
    for row in anomalies:
        try:
            prob_value = extract_prob_value(row.probability)
            alert = {
                "ip": str(row.src_ip),
                "prob": prob_value,
                "time": datetime.now().isoformat()
            }
            kafka_producer.send("alerts", alert)
        except Exception as e:
            print(f"Error sending alert: {e}")
    
    if kafka_producer:
        kafka_producer.flush()

query2 = predictions_with_prob.writeStream \
    .foreachBatch(send_alert_batch) \
    .start()

query1.awaitTermination()
query2.awaitTermination()