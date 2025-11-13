# scripts/streaming_predict.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, schema_of_json
from pyspark.ml import PipelineModel
import json

def start_streaming_prediction(model_path, kafka_bootstrap, kafka_topic, cassandra_host):
    spark = SparkSession.builder \
        .appName("Real-time Anomaly Detection") \
        .config("spark.cassandra.connection.host", cassandra_host) \
        .getOrCreate()

    # load model
    model = PipelineModel.load(model_path)

    # read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", kafka_topic) \
        .load()

    # to JSON
    schema = "SRC_IP_INT INT, DST_IP_INT INT, L4_SRC_PORT INT, L4_DST_PORT INT, PROTOCOL INT, " \
             "IN_BYTES LONG, OUT_BYTES LONG, FLOW_DURATION_MILLISECONDS LONG, Label_Num INT"

    parsed = df.selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")

    # predictions
    predictions = model.transform(parsed)

    # predictions
    alerts = predictions.filter(col("prediction") == 1) \
        .select("SRC_IP_INT", "DST_IP_INT", "L4_SRC_PORT", "L4_DST_PORT", "prediction")

    # write to Cassandra
    query1 = predictions.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="predictions", keyspace="netflow") \
        .outputMode("append") \
        .start()

    # send to Kafka
    query2 = alerts.selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("topic", "alerts") \
        .outputMode("append") \
        .start()

    query1.awaitTermination()
    query2.awaitTermination()

# rum
if __name__ == "__main__":
    start_streaming_prediction(
        model_path="/models/anomaly_model_rf.spark",
        kafka_bootstrap="kafka:9092",
        kafka_topic="netflow-data",
        cassandra_host="cassandra"
    )