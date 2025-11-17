# scripts/streaming_predict.py
# Real-time NetFlow processing: Save every flow + Predict anomalies simultaneously

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

def start_streaming_prediction():
    # ===================================================================
    # 1. Spark Session with Cassandra & Kafka connectivity
    # ===================================================================
    spark = SparkSession.builder \
        .appName("RealTime-NetFlow-Processing-And-Prediction") \
        .config("spark.cassandra.connection.host", "cassandra") \
        .config("spark.cassandra.auth.username", "cassandra") \
        .config("spark.cassandra.auth.password", "cassandra") \
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
        .getOrCreate()

    # Reduce log noise
    spark.sparkContext.setLogLevel("WARN")

    # ===================================================================
    # 2. Load the trained Random Forest (or any) model
    # ===================================================================
    print("Loading anomaly detection model...")
    model = PipelineModel.load("/model/anomaly_model_rf.spark")

    # ===================================================================
    # 3. Read streaming data from Kafka topic "netflow-data"
    # ===================================================================
    raw_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "netflow-data") \
        .option("startingOffsets", "latest") \
        .load()

    # ===================================================================
    # 4. Define the exact schema of incoming JSON messages
    # ===================================================================
    flow_schema = StructType([
        StructField("SRC_IP_INT", IntegerType(), True),
        StructField("DST_IP_INT", IntegerType(), True),
        StructField("L4_SRC_PORT", IntegerType(), True),
        StructField("L4_DST_PORT", IntegerType(), True),
        StructField("PROTOCOL", IntegerType(), True),
        StructField("IN_BYTES", LongType(), True),
        StructField("OUT_BYTES", LongType(), True),
        StructField("FLOW_DURATION_MILLISECONDS", LongType(), True),
        StructField("Label_Num", IntegerType(), True)        # original label (optional)
    ])

    # ===================================================================
    # 5. Parse JSON string coming from Kafka
    # ===================================================================
    parsed_df = raw_df \
        .selectExpr("CAST(value AS STRING) AS json") \
        .select(from_json(col("json"), flow_schema).alias("data")) \
        .select("data.*")

    # Add ingestion timestamp (very useful for time-window queries)
    parsed_df = parsed_df.withColumn("timestamp", current_timestamp())

    # ===================================================================
    # 6. Real-time prediction using the loaded model
    # ===================================================================
    predictions_df = model.transform(parsed_df)

    # ===================================================================
    # 7. Write EVERY flow to Cassandra (historical table: flows)
    # ===================================================================
    historical_query = predictions_df \
        .writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="flows", keyspace="netflow") \
        .option("checkpointLocation", "/tmp/cp_flows") \
        .outputMode("append") \
        .start()

    # ===================================================================
    # 8. Write predictions to Cassandra (real-time table: predictions)
    # ===================================================================
    prediction_query = predictions_df \
        .select(
            "SRC_IP_INT", "timestamp", "DST_IP_INT",
            "L4_SRC_PORT", "L4_DST_PORT", "PROTOCOL",
            "IN_BYTES", "OUT_BYTES", "FLOW_DURATION_MILLISECONDS",
            "prediction"
        ) \
        .writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="predictions", keyspace="netflow") \
        .option("checkpointLocation", "/tmp/cp_predictions") \
        .outputMode("append") \
        .start()

    # ===================================================================
    # 9. Send only malicious flows (prediction == 1) to Kafka topic "alerts"
    # ===================================================================
    alerts_df = predictions_df \
        .filter(col("prediction") == 1) \
        .selectExpr(
            "SRC_IP_INT as src_ip_int",
            "DST_IP_INT as dst_ip_int",
            "L4_SRC_PORT as src_port",
            "L4_DST_PORT as dst_port",
            "PROTOCOL as protocol",
            "IN_BYTES as in_bytes",
            "OUT_BYTES as out_bytes",
            "FLOW_DURATION_MILLISECONDS as duration_ms",
            "prediction",
            "timestamp"
        )

    alerts_query = alerts_df \
        .selectExpr("to_json(struct(*)) AS value") \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("topic", "alerts") \
        .option("checkpointLocation", "/tmp/cp_alerts") \
        .outputMode("append") \
        .start()

    # ===================================================================
    # 10. Start everything and keep the application alive
    # ===================================================================
    print("Real-time pipeline started!")
    print("→ All flows        → Cassandra netflow.flows")
    print("→ Predictions      → Cassandra netflow.predictions")
    print("→ Attack alerts    → Kafka topic 'alerts'")

    # Wait for all streams to finish (they never do in production)
    historical_query.awaitTermination()
    prediction_query.awaitTermination()
    alerts_query.awaitTermination()


if __name__ == "__main__":
    start_streaming_prediction()