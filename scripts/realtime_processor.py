# /scripts/realtime_processor_kafka.py
# يقرأ من Kafka → يتنبأ → يطبع → يحفظ في Cassandra

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, from_json, col
from pyspark.sql.types import *
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from kafka import KafkaConsumer
import json
from datetime import datetime

# Spark Session مع Kafka + Cassandra
spark = SparkSession.builder \
    .appName("RealTime IDS - Kafka → Spark → Cassandra") \
    .master("local[*]") \
    .config("spark.driver.memory", "6g") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

print("Loading ML Model...")
model = RandomForestClassificationModel.load("/model/anomaly_detection_model_rf.spark")
print("Model loaded successfully!")

# Vector Assembler
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

# Kafka Consumer
consumer = KafkaConsumer(
    'packets_raw',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='ids-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Real-time IDS Processor STARTED – Listening to Kafka topic: packets_raw")

batch = []

for message in consumer:
    pkt = message.value

    row = {
        "L4_SRC_PORT": float(pkt["src_port"]),
        "L4_DST_PORT": float(pkt["dst_port"]),
        "PROTOCOL": float(pkt["protocol"]),
        "L7_PROTO": 443.0 if pkt["dst_port"] == 443 else 80.0 if pkt["dst_port"] in [80, 8080] else 0.0,
        "IN_BYTES": float(pkt["pkt_len"] * 18),
        "IN_PKTS": 18.0,
        "OUT_BYTES": float(pkt["pkt_len"] * 14),
        "OUT_PKTS": 14.0,
        "TCP_FLAGS": float(pkt["tcp_flags"]),
        "CLIENT_TCP_FLAGS": float(pkt["tcp_flags"]),
        "SERVER_TCP_FLAGS": float(pkt["tcp_flags"]),
        "FLOW_DURATION_MILLISECONDS": 1000.0,
        "DURATION_IN": 500.0,
        "DURATION_OUT": 500.0,
        "MIN_TTL": float(pkt.get("ttl", 64)),
        "MAX_TTL": float(pkt.get("ttl", 64)),
        "LONGEST_FLOW_PKT": float(pkt["pkt_len"]),
        "SHORTEST_FLOW_PKT": float(pkt["pkt_len"]),
        "MIN_IP_PKT_LEN": float(pkt["pkt_len"]),
        "MAX_IP_PKT_LEN": float(pkt["pkt_len"]),
        "SRC_TO_DST_SECOND_BYTES": float(pkt["pkt_len"] * 25),
        "DST_TO_SRC_SECOND_BYTES": float(pkt["pkt_len"] * 20),
        "RETRANSMITTED_IN_BYTES": 0.0, "RETRANSMITTED_IN_PKTS": 0.0,
        "RETRANSMITTED_OUT_BYTES": 0.0, "RETRANSMITTED_OUT_PKTS": 0.0,
        "SRC_TO_DST_AVG_THROUGHPUT": 8000000.0,
        "DST_TO_SRC_AVG_THROUGHPUT": 7000000.0,
        "NUM_PKTS_UP_TO_128_BYTES": 12.0 if pkt["pkt_len"] <= 128 else 0.0,
        "NUM_PKTS_128_TO_256_BYTES": 4.0 if 128 < pkt["pkt_len"] <= 256 else 0.0,
        "NUM_PKTS_256_TO_512_BYTES": 2.0 if 256 < pkt["pkt_len"] <= 512 else 0.0,
        "NUM_PKTS_512_TO_1024_BYTES": 1.0 if 512 < pkt["pkt_len"] <= 1024 else 0.0,
        "NUM_PKTS_1024_TO_1514_BYTES": 1.0 if pkt["pkt_len"] > 1024 else 0.0,
        "TCP_WIN_MAX_IN": 65535.0, "TCP_WIN_MAX_OUT": 65535.0,
        "ICMP_TYPE": 8.0 if pkt["protocol"] == 1 else 0.0,
        "ICMP_IPV4_TYPE": 8.0 if pkt["protocol"] == 1 else 0.0,
        "DNS_QUERY_ID": 0.0, "DNS_QUERY_TYPE": 0.0, "DNS_TTL_ANSWER": 0.0, "FTP_COMMAND_RET_CODE": 0.0
    }

    try:
        df = spark.createDataFrame([row])
        prediction = model.transform(assembler.transform(df)).collect()[0]
        score = float(prediction.probability[1])
        is_anomaly = int(prediction.prediction)

        label = "ATTACK" if is_anomaly else "BENIGN"
        print(f"\n[{datetime.now().strftime('%H:%M:%S')}] {pkt['src_ip']}:{pkt['src_port']} → {pkt['dst_ip']}:{pkt['dst_port']}")
        print(f" PREDICTION → {label} | Score: {score:.4f}")
        if is_anomaly:
            print(" MALICIOUS TRAFFIC DETECTED & LOGGED!")

        # إضافة للباتش لحفظها في Cassandra
        batch.append({
            "src_ip_int": pkt["src_ip_int"],
            "ingestion_time": datetime.now(),
            "dst_ip_int": pkt["dst_ip_int"],
            "src_port": pkt["src_port"],
            "dst_port": pkt["dst_port"],
            "protocol": pkt["protocol"],
            "in_bytes": pkt["pkt_len"] * 18,
            "out_bytes": pkt["pkt_len"] * 14,
            "duration_ms": 1000,
            "is_anomaly": is_anomaly,
            "anomaly_score": score
        })

        # حفظ كل 20 تنبؤ
        if len(batch) >= 20:
            spark.createDataFrame(batch).write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="predictions", keyspace="netflow") \
                .mode("append") \
                .save()
            print(f" Saved {len(batch)} predictions to Cassandra")
            batch.clear()

    except Exception as e:
        print(f"Prediction error: {e}")