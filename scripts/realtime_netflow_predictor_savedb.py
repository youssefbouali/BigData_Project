from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from scapy.all import sniff, IP, TCP, UDP, ICMP
import time
from datetime import datetime

# Spark Session
spark = SparkSession.builder \
    .appName("IDS Processor Only") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .getOrCreate()

print("Loading model from /model/anomaly_detection_model_rf.spark ...")
model = RandomForestClassificationModel.load("/model/anomaly_detection_model_rf.spark")
print("Model loaded successfully!")

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

# إضافة batch كمتغير global
batch = []

def predict_packet(pkt):
    if not pkt.haslayer(IP):
        return

    ip = pkt[IP]
    src_ip = ip.src
    dst_ip = ip.dst
    proto = ip.proto
    pkt_len = len(pkt)
    ttl = ip.ttl

    # Ports
    src_port = dst_port = 0
    tcp_flags = 0
    if pkt.haslayer(TCP):
        src_port = pkt[TCP].sport
        dst_port = pkt[TCP].dport
        tcp_flags = int(pkt[TCP].flags)
    elif pkt.haslayer(UDP):
        src_port = pkt[UDP].sport
        dst_port = pkt[UDP].dport
    elif pkt.haslayer(ICMP):
        proto = 1

    # إنشاء DataFrame
    df = spark.range(1).drop("id")

    df = df.withColumn("L4_SRC_PORT", lit(src_port)) \
           .withColumn("L4_DST_PORT", lit(dst_port)) \
           .withColumn("PROTOCOL", lit(proto)) \
           .withColumn("L7_PROTO", lit(80.0 if dst_port in [80,443] else 0.0)) \
           .withColumn("IN_BYTES", lit(pkt_len * 15)) \
           .withColumn("IN_PKTS", lit(15)) \
           .withColumn("OUT_BYTES", lit(pkt_len * 12)) \
           .withColumn("OUT_PKTS", lit(12)) \
           .withColumn("TCP_FLAGS", lit(tcp_flags)) \
           .withColumn("CLIENT_TCP_FLAGS", lit(tcp_flags)) \
           .withColumn("SERVER_TCP_FLAGS", lit(tcp_flags)) \
           .withColumn("FLOW_DURATION_MILLISECONDS", lit(800)) \
           .withColumn("DURATION_IN", lit(400)) \
           .withColumn("DURATION_OUT", lit(400)) \
           .withColumn("MIN_TTL", lit(ttl)) \
           .withColumn("MAX_TTL", lit(ttl)) \
           .withColumn("LONGEST_FLOW_PKT", lit(pkt_len)) \
           .withColumn("SHORTEST_FLOW_PKT", lit(pkt_len)) \
           .withColumn("MIN_IP_PKT_LEN", lit(pkt_len)) \
           .withColumn("MAX_IP_PKT_LEN", lit(pkt_len)) \
           .withColumn("SRC_TO_DST_SECOND_BYTES", lit(pkt_len * 20.0)) \
           .withColumn("DST_TO_SRC_SECOND_BYTES", lit(pkt_len * 15.0)) \
           .withColumn("RETRANSMITTED_IN_BYTES", lit(0)) \
           .withColumn("RETRANSMITTED_IN_PKTS", lit(0)) \
           .withColumn("RETRANSMITTED_OUT_BYTES", lit(0)) \
           .withColumn("RETRANSMITTED_OUT_PKTS", lit(0)) \
           .withColumn("SRC_TO_DST_AVG_THROUGHPUT", lit(5000000.0)) \
           .withColumn("DST_TO_SRC_AVG_THROUGHPUT", lit(4000000.0)) \
           .withColumn("NUM_PKTS_UP_TO_128_BYTES", lit(10 if pkt_len <= 128 else 0)) \
           .withColumn("NUM_PKTS_128_TO_256_BYTES", lit(3 if 128 < pkt_len <= 256 else 0)) \
           .withColumn("NUM_PKTS_256_TO_512_BYTES", lit(1 if 256 < pkt_len <= 512 else 0)) \
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

    # Vector + Predict
    try:
        vec = assembler.transform(df)
        result = model.transform(vec).select("prediction", "probability").collect()[0]
        label = "ATTACK" if result.prediction == 1 else "BENIGN"
        prob = float(result.probability[1]) * 100

        # إصلاح عملية حفظ البيانات
        record = {
            "src_ip": src_ip,
            "dst_ip": dst_ip,
            "src_port": src_port,
            "dst_port": dst_port,
            "protocol": proto,
            "is_anomaly": int(result.prediction),
            "anomaly_score": prob,  # استخدام prob بدلاً من score
            "ingestion_time": datetime.now()
        }
        
        batch.append(record)

        if len(batch) >= 10:
            spark.createDataFrame(batch).write \
                .format("org.apache.spark.sql.cassandra") \
                .option("table", "predictions") \
                .option("keyspace", "netflow") \
                .mode("append").save()
            print(f"Saved {len(batch)} records to Cassandra")
            batch.clear()  # استخدام clear بدلاً من تعيين قائمة فارغة

        print("\n" + "="*90)
        print(f" REAL-TIME DETECTION | {src_ip}:{src_port} → {dst_ip}:{dst_port} (Proto: {proto})")
        print(f" PREDICTION → {label} | Attack Probability: {prob:.2f}%")
        if label == "ATTACK":
            print(" MALICIOUS TRAFFIC DETECTED! BLOCK THIS FLOW NOW!")
        print("="*90)
    except Exception as e:
        print(f"Prediction error: {e}")

# بدء التقاط الباكتات
print("Starting real-time detection (every packet triggers prediction)...")
print("Try: curl google.com   OR   hping3 --flood ...   inside/outside the container")
sniff(prn=predict_packet, store=False, iface="eth0")

spark.stop()