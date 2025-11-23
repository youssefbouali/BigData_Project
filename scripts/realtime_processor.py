# /scripts/realtime_processor.py
# اللي فيه Spark + Model + Cassandra – آمن تمامًا
import socket
import struct
import os
import time
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler

# إعداد Spark
spark = SparkSession.builder \
    .appName("RealTime Processor") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

print("Loading model...")
model = RandomForestClassificationModel.load("/model/anomaly_detection_model_rf.spark")
print("Model loaded!")

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

# إنشاء socket لاستقبال البيانات
if os.path.exists("/tmp/spark_detector.sock"):
    os.remove("/tmp/spark_detector.sock")

server = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
server.bind("/tmp/spark_detector.sock")
server.settimeout(1)

batch = []

while True:
    try:
        data, _ = server.recvfrom(28)  # 7 integers × 4 bytes
        if len(data) != 28: continue
        src_ip_int, dst_ip_int, src_port, dst_port, proto, pkt_len, tcp_flags = struct.unpack("!IIIIIiI", data)

        row = {k: 0.0 for k in assembler.getInputCols()}
        row.update({
            "L4_SRC_PORT": float(src_port),
            "L4_DST_PORT": float(dst_port),
            "PROTOCOL": float(proto),
            "L7_PROTO": 443.0 if dst_port == 443 else 80.0 if dst_port in [80,8080] else 0.0,
            "IN_BYTES": float(pkt_len * 18),
            "IN_PKTS": 18.0,
            "OUT_BYTES": float(pkt_len * 14),
            "OUT_PKTS": 14.0,
            "TCP_FLAGS": float(tcp_flags),
            "CLIENT_TCP_FLAGS": float(tcp_flags),
            "SERVER_TCP_FLAGS": float(tcp_flags),
            "FLOW_DURATION_MILLISECONDS": 1000.0,
            "DURATION_IN": 500.0, "DURATION_OUT": 500.0,
            "MIN_TTL": 64.0, "MAX_TTL": 64.0,
            "LONGEST_FLOW_PKT": float(pkt_len), "SHORTEST_FLOW_PKT": float(pkt_len),
            "MIN_IP_PKT_LEN": float(pkt_len), "MAX_IP_PKT_LEN": float(pkt_len),
            "SRC_TO_DST_SECOND_BYTES": float(pkt_len * 25),
            "DST_TO_SRC_SECOND_BYTES": float(pkt_len * 20),
            "SRC_TO_DST_AVG_THROUGHPUT": 8000000.0,
            "DST_TO_SRC_AVG_THROUGHPUT": 7000000.0,
            "NUM_PKTS_UP_TO_128_BYTES": 12.0 if pkt_len <= 128 else 0.0,
            "NUM_PKTS_128_TO_256_BYTES": 4.0 if 128 < pkt_len <= 256 else 0.0,
            "NUM_PKTS_256_TO_512_BYTES": 2.0 if 256 < pkt_len <= 512 else 0.0,
            "NUM_PKTS_512_TO_1024_BYTES": 1.0 if 512 < pkt_len <= 1024 else 0.0,
            "NUM_PKTS_1024_TO_1514_BYTES": 1.0 if pkt_len > 1024 else 0.0,
            "TCP_WIN_MAX_IN": 65535.0, "TCP_WIN_MAX_OUT": 65535.0,
            "ICMP_TYPE": 8.0 if proto == 1 else 0.0,
            "ICMP_IPV4_TYPE": 8.0 if proto == 1 else 0.0,
        })

        df = spark.createDataFrame([row])
        pred = model.transform(assembler.transform(df)).select("prediction", "probability").collect()[0]
        score = float(pred.probability[1])
        label = "ATTACK" if pred.prediction == 1 else "BENIGN"

        print(f"[{datetime.now().strftime('%H:%M:%S')}] {src_ip_int} → {dst_ip_int}:{dst_port} | {label} ({score:.4f})")
        if pred.prediction == 1:
            print(" MALICIOUS TRAFFIC DETECTED!")

        # حفظ
        save_row = {
            "src_ip_int": src_ip_int,
            "dst_ip_int": dst_ip_int,
            "src_port": src_port,
            "dst_port": dst_port,
            "protocol": proto,
            "in_bytes": pkt_len * 18,
            "out_bytes": pkt_len * 14,
            "duration_ms": 1000,
            "is_anomaly": int(pred.prediction),
            "anomaly_score": score,
            "ingestion_time": datetime.now()
        }
        batch.append(save_row)

        if len(batch) >= 10:
            spark.createDataFrame(batch).write \
                .format("org.apache.spark.sql.cassandra") \
                .options(table="predictions", keyspace="netflow") \
                .mode("append").save()
            print(f"Saved {len(batch)} predictions")
            batch.clear()

    except socket.timeout:
        continue
    except Exception as e:
        print(f"Error: {e}")