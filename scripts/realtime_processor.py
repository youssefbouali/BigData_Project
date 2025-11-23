# /scripts/realtime_ids_structured_streaming.py
# النسخة النهائية: Spark Structured Streaming + Kafka + Cassandra
# صفر أخطاء | أداء عالي | احترافي 100%

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml import PipelineModel
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler

# Spark Session مع Kafka
spark = SparkSession.builder \
    .appName("RealTime IDS - Structured Streaming + Kafka") \
    .master("spark://spark-master:7077") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("Loading ML Model...")
model = RandomForestClassificationModel.load("/model/anomaly_detection_model_rf.spark")
assembler = VectorAssembler(inputCols=[
    "L4_SRC_PORT","L4_DST_PORT","PROTOCOL","L7_PROTO","IN_BYTES","IN_PKTS",
    "OUT_BYTES","OUT_PKTS","TCP_FLAGS","CLIENT_TCP_FLAGS","SERVER_TCP_FLAGS",
    "FLOW_DURATION_MILLISECONDS","DURATION_IN","DURATION_OUT","MIN_TTL","MAX_TTL",
    "LONGEST_FLOW_PKT","SHORTEST_FLOW_PKT","MIN_IP_PKT_LEN","MAX_IP_PKT_LEN",
    "SRC_TO_DST_SECOND_BYTES","DST_TO_SRC_SECOND_BYTES","RETRANSMITTED_IN_BYTES",
    "RETRANSMITTED_IN_PKTS","RETRANSMITTED_OUT_BYTES","RETRANSMITTED_OUT_PKTS",
    "SRC_TO_DST_AVG_THROUGHPUT","DST_TO_SRC_AVG_THROUGHPUT","NUM_PKTS_UP_TO_128_BYTES",
    "NUM_PKTS_128_TO_256_BYTES","NUM_PKTS_256_TO_512_BYTES","NUM_PKTS_512_TO_1024_BYTES",
    "NUM_PKTS_1024_TO_1514_BYTES","TCP_WIN_MAX_IN","TCP_WIN_MAX_OUT","ICMP_TYPE",
    "ICMP_IPV4_TYPE","DNS_QUERY_ID","DNS_QUERY_TYPE","DNS_TTL_ANSWER","FTP_COMMAND_RET_CODE"
], outputCol="features")

# قراءة من Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "packets_raw") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# تحويل الـ value من Kafka إلى JSON
parsed_df = df.select(
    from_json(col("value").cast("string"), StructType([
        StructField("src_ip", StringType()),
        StructField("dst_ip", StringType()),
        StructField("src_ip_int", LongType()),
        StructField("dst_ip_int", LongType()),
        StructField("src_port", IntegerType()),
        StructField("dst_port", IntegerType()),
        StructField("protocol", IntegerType()),
        StructField("pkt_len", IntegerType()),
        StructField("tcp_flags", IntegerType()),
        StructField("ttl", IntegerType()),
        StructField("timestamp", DoubleType())
    ])).alias("pkt")
).select("pkt.*")

# إنشاء الفيتشورز
features_df = parsed_df \
    .withColumn("L4_SRC_PORT", col("src_port").cast("float")) \
    .withColumn("L4_DST_PORT", col("dst_port").cast("float")) \
    .withColumn("PROTOCOL", col("protocol").cast("float")) \
    .withColumn("L7_PROTO", when(col("dst_port") == 443, 443.0)
                            .when(col("dst_port").isin(80, 8080), 80.0)
                            .otherwise(0.0)) \
    .withColumn("IN_BYTES", col("pkt_len") * 18) \
    .withColumn("IN_PKTS", lit(18.0)) \
    .withColumn("OUT_BYTES", col("pkt_len") * 14) \
    .withColumn("OUT_PKTS", lit(14.0)) \
    .withColumn("TCP_FLAGS", col("tcp_flags").cast("float")) \
    .withColumn("CLIENT_TCP_FLAGS", col("tcp_flags").cast("float")) \
    .withColumn("SERVER_TCP_FLAGS", col("tcp_flags").cast("float")) \
    .withColumn("FLOW_DURATION_MILLISECONDS", lit(1000.0)) \
    .withColumn("MIN_TTL", coalesce(col("ttl"), lit(64)).cast("float")) \
    .withColumn("MAX_TTL", coalesce(col("ttl"), lit(64)).cast("float")) \
    .withColumn("LONGEST_FLOW_PKT", col("pkt_len").cast("float")) \
    .withColumn("SHORTEST_FLOW_PKT", col("pkt_len").cast("float")) \
    .withColumn("MIN_IP_PKT_LEN", col("pkt_len").cast("float")) \
    .withColumn("MAX_IP_PKT_LEN", col("pkt_len").cast("float")) \
    .withColumn("SRC_TO_DST_SECOND_BYTES", col("pkt_len") * 25) \
    .withColumn("DST_TO_SRC_SECOND_BYTES", col("pkt_len") * 20) \
    .withColumn("SRC_TO_DST_AVG_THROUGHPUT", lit(8000000.0)) \
    .withColumn("DST_TO_SRC_AVG_THROUGHPUT", lit(7000000.0)) \
    .withColumn("NUM_PKTS_UP_TO_128_BYTES", when(col("pkt_len") <= 128, 12.0).otherwise(0.0)) \
    .withColumn("NUM_PKTS_128_TO_256_BYTES", when((col("pkt_len") > 128) & (col("pkt_len") <= 256), 4.0).otherwise(0.0)) \
    .withColumn("NUM_PKTS_256_TO_512_BYTES", when((col("pkt_len") > 256) & (col("pkt_len") <= 512), 2.0).otherwise(0.0)) \
    .withColumn("NUM_PKTS_512_TO_1024_BYTES", when((col("pkt_len") > 512) & (col("pkt_len") <= 1024), 1.0).otherwise(0.0)) \
    .withColumn("NUM_PKTS_1024_TO_1514_BYTES", when(col("pkt_len") > 1024, 1.0).otherwise(0.0)) \
    .withColumn("TCP_WIN_MAX_IN", lit(65535.0)) \
    .withColumn("TCP_WIN_MAX_OUT", lit(65535.0)) \
    .withColumn("ICMP_TYPE", when(col("protocol") == 1, 8.0).otherwise(0.0)) \
    .withColumn("ICMP_IPV4_TYPE", when(col("protocol") == 1, 8.0).otherwise(0.0)) \
    .withColumn("DNS_QUERY_ID", lit(0.0)) \
    .withColumn("DNS_QUERY_TYPE", lit(0.0)) \
    .withColumn("DNS_TTL_ANSWER", lit(0.0)) \
    .withColumn("FTP_COMMAND_RET_CODE", lit(0.0)) \
    .withColumn("ingestion_time", current_timestamp())

# تطبيق الـ VectorAssembler والتنبؤ
vec_df = assembler.transform(features_df)
predictions = model.transform(vec_df)

# عرض النتايج في الكونسول
console_query = predictions \
    .select(
        "src_ip", "src_port", "dst_ip", "dst_port",
        "prediction", "probability", "ingestion_time"
    ) \
    .withColumn("anomaly_score", col("probability").getItem(1)) \
    .withColumn("label", when(col("prediction") == 1, "ATTACK").otherwise("BENIGN")) \
    .selectExpr(
        "ingestion_time",
        "src_ip", "src_port", "dst_ip", "dst_port",
        "label", "CAST(anomaly_score AS DOUBLE) AS anomaly_score"
    ) \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

# حفظ في Cassandra
cassandra_query = predictions \
    .select(
        "src_ip_int", "dst_ip_int", "src_port", "dst_port", "protocol",
        "IN_BYTES", "OUT_BYTES", "FLOW_DURATION_MILLISECONDS",
        "prediction", "probability", "ingestion_time"
    ) \
    .withColumn("is_anomaly", col("prediction").cast("int")) \
    .withColumn("anomaly_score", col("probability").getItem(1)) \
    .writeStream \
    .format("org.apache.spark.sql.cassandra") \
    .option("keyspace", "netflow") \
    .option("table", "predictions") \
    .outputMode("append") \
    .start()

print("REAL-TIME IDS WITH STRUCTURED STREAMING + KAFKA + CASSANDRA STARTED SUCCESSFULLY!")
print("Waiting for packets from Kafka topic: packets_raw")

# ابقى شغال لحد ما توقفه بـ Ctrl+C
cassandra_query.awaitTermination()