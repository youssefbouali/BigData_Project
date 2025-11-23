# /scripts/realtime_processor_final.py
# شغال مع Spark 3.3.0 بدون أي مشكلة، بدون Structured Streaming، بدون cloudpickle crash

from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from kafka import KafkaConsumer
import json
from datetime import datetime

# Spark Session عادي جدًا (local mode)
spark = SparkSession.builder \
    .appName("RealTime IDS - Final Version") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

print("Loading model...")
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

consumer = KafkaConsumer(
    'packets_raw',
    bootstrap_servers=['kafka:9092'],
    auto_offset_reset='earliest',      # ← ده اللي كان ناقص
    enable_auto_commit=True,
    group_id='ids-group',              # ← خليها ثابتة دايمًا
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

batch = []
print("REAL-TIME PROCESSOR STARTED – Waiting for packets...")

for message in consumer:
    pkt = message.value

    row = {col: 0.0 for col in assembler.getInputCols()}
    row.update({
        "L4_SRC_PORT": float(pkt.get("src_port", 0)),
        "L4_DST_PORT": float(pkt.get("dst_port", 0)),
        "PROTOCOL": float(pkt.get("protocol", 0)),
        "L7_PROTO": 443.0 if pkt.get("dst_port") == 443 else 80.0 if pkt.get("dst_port") in [80,8080] else 0.0,
        "IN_BYTES": float(pkt.get("pkt_len", 0) * 18),
        "IN_PKTS": 18.0,
        "OUT_BYTES": float(pkt.get("pkt_len", 0) * 14),
        "OUT_PKTS": 14.0,
        "TCP_FLAGS": float(pkt.get("tcp_flags", 0)),
        "CLIENT_TCP_FLAGS": float(pkt.get("tcp_flags", 0)),
        "SERVER_TCP_FLAGS": float(pkt.get("tcp_flags", 0)),
        "FLOW_DURATION_MILLISECONDS": 1000.0,
        "MIN_TTL": float(pkt.get("ttl", 64)),
        "MAX_TTL": float(pkt.get("ttl", 64)),
        "LONGEST_FLOW_PKT": float(pkt.get("pkt_len", 0)),
        "SHORTEST_FLOW_PKT": float(pkt.get("pkt_len", 0)),
        "MIN_IP_PKT_LEN": float(pkt.get("pkt_len", 0)),
        "MAX_IP_PKT_LEN": float(pkt.get("pkt_len", 0)),
        "SRC_TO_DST_SECOND_BYTES": float(pkt.get("pkt_len", 0) * 25),
        "DST_TO_SRC_SECOND_BYTES": float(pkt.get("pkt_len", 0) * 20),
        "SRC_TO_DST_AVG_THROUGHPUT": 8000000.0,
        "DST_TO_SRC_AVG_THROUGHPUT": 7000000.0,
        "NUM_PKTS_UP_TO_128_BYTES": 12.0 if pkt.get("pkt_len", 0) <= 128 else 0.0,
        "NUM_PKTS_128_TO_256_BYTES": 4.0 if 128 < pkt.get("pkt_len", 0) <= 256 else 0.0,
        "NUM_PKTS_256_TO_512_BYTES": 2.0 if 256 < pkt.get("pkt_len", 0) <= 512 else 0.0,
        "NUM_PKTS_512_TO_1024_BYTES": 1.0 if 512 < pkt.get("pkt_len", 0) <= 1024 else 0.0,
        "NUM_PKTS_1024_TO_1514_BYTES": 1.0 if pkt.get("pkt_len", 0) > 1024 else 0.0,
        "TCP_WIN_MAX_IN": 65535.0,
        "TCP_WIN_MAX_OUT": 65535.0,
        "ICMP_TYPE": 8.0 if pkt.get("protocol") == 1 else 0.0,
        "ICMP_IPV4_TYPE": 8.0 if pkt.get("protocol") == 1 else 0.0,
    })

    df = spark.createDataFrame([row])
    pred = model.transform(assembler.transform(df)).collect()[0]
    score = float(pred.probability[1])
    label = "ATTACK" if pred.prediction == 1 else "BENIGN"

    print(f"[{datetime.now().strftime('%H:%M:%S')}] {pkt['src_ip']}:{pkt['src_port']} → {pkt['dst_ip']}:{pkt['dst_port']} | {label} ({score:.4f})")
    if pred.prediction == 1:
        print(" MALICIOUS TRAFFIC DETECTED!")

    batch.append({
        "src_ip_int": pkt["src_ip_int"],
        "dst_ip_int": pkt["dst_ip_int"],
        "src_port": pkt["src_port"],
        "dst_port": pkt["dst_port"],
        "protocol": pkt["protocol"],
        "in_bytes": pkt["pkt_len"] * 18,
        "out_bytes": pkt["pkt_len"] * 14,
        "duration_ms": 1000,
        "is_anomaly": int(pred.prediction),
        "anomaly_score": score,
        "ingestion_time": datetime.now()
    })

    if len(batch) >= 15:
        spark.createDataFrame(batch).write \
            .format("org.apache.spark.sql.cassandra") \
            .option("table", "predictions") \
            .option("keyspace", "netflow") \
            .mode("append").save()
        print(f" Saved {len(batch)} records to Cassandra")
        batch.clear()