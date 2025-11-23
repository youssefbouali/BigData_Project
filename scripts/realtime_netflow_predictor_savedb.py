# /scripts/realtime_netflow_predictor_final.py
# نسخة نهائية 100% – تكشف + تحفظ كل تنبؤ في Cassandra (netflow.predictions)
# تعمل داخل spark-master بدون أي تعديل في docker-compose

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp, expr, regexp_extract, col
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from scapy.all import sniff, IP, TCP, UDP, ICMP
import time

# ===================================================================
# Spark Session + Cassandra Connection
# ===================================================================
spark = SparkSession.builder \
    .appName("RealTime Anomaly Detector → Cassandra") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

print("Model loading...")
model = RandomForestClassificationModel.load("/model/anomaly_detection_model_rf.spark")
print("Model loaded successfully!")

# ===================================================================
# VectorAssembler (نفس ترتيب الفيتشورز في التدريب)
# ===================================================================
input_cols = [
    "L4_SRC_PORT", "L4_DST_PORT", "PROTOCOL", "L7_PROTO", "IN_BYTES", "IN_PKTS",
    "OUT_BYTES", "OUT_PKTS", "TCP_FLAGS", "CLIENT_TCP_FLAGS", "SERVER_TCP_FLAGS",
    "FLOW_DURATION_MILLISECONDS", "DURATION_IN", "DURATION_OUT", "MIN_TTL", "MAX_TTL",
    "LONGEST_FLOW_PKT", "SHORTEST_FLOW_PKT", "MIN_IP_PKT_LEN", "MAX_IP_PKT_LEN",
    "SRC_TO_DST_SECOND_BYTES", "DST_TO_SRC_SECOND_BYTES", "RETRANSMITTED_IN_BYTES",
    "RETRANSMITTED_IN_PKTS", "RETRANSMITTED_OUT_BYTES", "RETRANSMITTED_OUT_PKTS",
    "SRC_TO_DST_AVG_THROUGHPUT", "DST_TO_SRC_AVG_THROUGHPUT", "NUM_PKTS_UP_TO_128_BYTES",
    "NUM_PKTS_128_TO_256_BYTES", "NUM_PKTS_256_TO_512_BYTES", "NUM_PKTS_512_TO_1024_BYTES",
    "NUM_PKTS_1024_TO_1514_BYTES", "TCP_WIN_MAX_IN", "TCP_WIN_MAX_OUT", "ICMP_TYPE",
    "ICMP_IPV4_TYPE", "DNS_QUERY_ID", "DNS_QUERY_TYPE", "DNS_TTL_ANSWER", "FTP_COMMAND_RET_CODE"
]

assembler = VectorAssembler(inputCols=input_cols, outputCol="features")

# ===================================================================
# دالة تحويل IP → Integer (نفس اللي في clean_and_load.py)
# ===================================================================
def ip_to_int(ip_str):
    return int("".join([f"{int(x):03d}" for x in ip_str.split(".")])) if ip_str else 0

# ===================================================================
# دالة التنبؤ + الحفظ في Cassandra
# ===================================================================
def predict_and_save(pkt):
    if not pkt.haslayer(IP):
        return

    ip = pkt[IP]
    src_ip = ip.src
    dst_ip = ip.dst
    proto = ip.proto
    pkt_len = len(pkt)
    ttl = ip.ttl

    src_port = dst_port = 0
    tcp_flags = 0
    if pkt.haslayer(TCP):
        src_port = pkt[TCP].sport
        dst_port = pkt[TCP].dport
        tcp_flags = int(pkt[TCP].flags)
    elif pkt.haslayer(UDP):
        src_port = pkt[UDP].sport
        dst_port = pkt[UDP].dport

    # تحويل IP → Integer
    src_ip_int = ip_to_int(src_ip)
    dst_ip_int = ip_to_int(dst_ip)

    # بناء صف واحد
    df = spark.range(1).drop("id") \
        .withColumn("L4_SRC_PORT", lit(src_port)) \
        .withColumn("L4_DST_PORT", lit(dst_port)) \
        .withColumn("PROTOCOL", lit(proto)) \
        .withColumn("L7_PROTO", lit(443.0 if dst_port == 443 else 80.0 if dst_port == 80 else 0.0)) \
        .withColumn("IN_BYTES", lit(pkt_len * 18)) \
        .withColumn("IN_PKTS", lit(18)) \
        .withColumn("OUT_BYTES", lit(pkt_len * 14)) \
        .withColumn("OUT_PKTS", lit(14)) \
        .withColumn("TCP_FLAGS", lit(tcp_flags)) \
        .withColumn("CLIENT_TCP_FLAGS", lit(tcp_flags)) \
        .withColumn("SERVER_TCP_FLAGS", lit(tcp_flags)) \
        .withColumn("FLOW_DURATION_MILLISECONDS", lit(1000)) \
        .withColumn("DURATION_IN", lit(500)) \
        .withColumn("DURATION_OUT", lit(500)) \
        .withColumn("MIN_TTL", lit(ttl)) \
        .withColumn("MAX_TTL", lit(ttl)) \
        .withColumn("LONGEST_FLOW_PKT", lit(pkt_len)) \
        .withColumn("SHORTEST_FLOW_PKT", lit(pkt_len)) \
        .withColumn("MIN_IP_PKT_LEN", lit(pkt_len)) \
        .withColumn("MAX_IP_PKT_LEN", lit(pkt_len)) \
        .withColumn("SRC_TO_DST_SECOND_BYTES", lit(pkt_len * 25.0)) \
        .withColumn("DST_TO_SRC_SECOND_BYTES", lit(pkt_len * 20.0)) \
        .withColumn("RETRANSMITTED_IN_BYTES", lit(0)) \
        .withColumn("RETRANSMITTED_IN_PKTS", lit(0)) \
        .withColumn("RETRANSMITTED_OUT_BYTES", lit(0)) \
        .withColumn("RETRANSMITTED_OUT_PKTS", lit(0)) \
        .withColumn("SRC_TO_DST_AVG_THROUGHPUT", lit(8000000.0)) \
        .withColumn("DST_TO_SRC_AVG_THROUGHPUT", lit(7000000.0)) \
        .withColumn("NUM_PKTS_UP_TO_128_BYTES", lit(12 if pkt_len <= 128 else 0)) \
        .withColumn("NUM_PKTS_128_TO_256_BYTES", lit(4 if 128 < pkt_len <= 256 else 0)) \
        .withColumn("NUM_PKTS_256_TO_512_BYTES", lit(2 if 256 < pkt_len <= 512 else 0)) \
        .withColumn("NUM_PKTS_512_TO_1024_BYTES", lit(1 if 512 < pkt_len <= 1024 else 0)) \
        .withColumn("NUM_PKTS_1024_TO_1514_BYTES", lit(1 if pkt_len > 1024 else 0)) \
        .withColumn("TCP_WIN_MAX_IN", lit(65535)) \
        .withColumn("TCP_WIN_MAX_OUT", lit(65535)) \
        .withColumn("ICMP_TYPE", lit(8 if proto == 1 else 0)) \
        .withColumn("ICMP_IPV4_TYPE", lit(8 if proto == 1 else 0)) \
        .withColumn("DNS_QUERY_ID", lit(0)) \
        .withColumn("DNS_QUERY_TYPE", lit(0)) \
        .withColumn("DNS_TTL_ANSWER", lit(0)) \
        .withColumn("FTP_COMMAND_RET_CODE", lit(0.0))

    try:
        # تنبؤ
        vec = assembler.transform(df)
        result = model.transform(vec).select("prediction", "probability").collect()[0]
        is_anomaly = int(result.prediction)
        anomaly_score = float(result.probability[1])

        # إضافة البيانات المطلوبة
        prediction_df = df \
            .withColumn("src_ip_int", lit(src_ip_int)) \
            .withColumn("dst_ip_int", lit(dst_ip_int)) \
            .withColumn("src_port", lit(src_port)) \
            .withColumn("dst_port", lit(dst_port)) \
            .withColumn("protocol", lit(proto)) \
            .withColumn("in_bytes", lit(pkt_len * 18)) \
            .withColumn("out_bytes", lit(pkt_len * 14)) \
            .withColumn("duration_ms", lit(1000)) \
            .withColumn("is_anomaly", lit(is_anomaly)) \
            .withColumn("anomaly_score", lit(anomaly_score)) \
            .withColumn("ingestion_time", current_timestamp()) \
            .select(
                "src_ip_int", "ingestion_time", "dst_ip_int", "src_port", "dst_port",
                "protocol", "in_bytes", "out_bytes", "duration_ms",
                "is_anomaly", "anomaly_score"
            )

        # حفظ في Cassandra
        prediction_df.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="predictions", keyspace="netflow") \
            .mode("append") \
            .save()

        # طباعة التنبيه
        label = "ATTACK" if is_anomaly == 1 else "BENIGN"
        print("\n" + "="*100)
        print(f" DETECTED | {src_ip}:{src_port} → {dst_ip}:{dst_port} | Proto: {proto}")
        print(f" PREDICTION → {label} | Score: {anomaly_score:.4f}")
        if is_anomaly == 1:
            print(" MALICIOUS TRAFFIC DETECTED & SAVED TO CASSANDRA!")
        print("="*100)

    except Exception as e:
        print(f"Error: {e}")

# ===================================================================
# بدء التقاط الباكتات
# ===================================================================
print("Real-time Anomaly Detector + Cassandra Logging STARTED")
print("Every detection is saved in netflow.predictions")
sniff(prn=predict_and_save, store=False, iface="eth0")