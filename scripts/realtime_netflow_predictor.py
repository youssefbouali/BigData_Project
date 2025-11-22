# realtime_netflow_predictor.py
# Real-time NetFlow / Packet → Spark ML Model Inference (Zero pickling, Super Fast)

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from scapy.all import sniff, IP, TCP, UDP
import threading
import time

# ====================== Spark Setup (Once) ======================
spark = SparkSession.builder \
    .appName("RealTime NetFlow Anomaly Detector") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
    .getOrCreate()

# تحميل الموديل مرة واحدة فقط
print("Loading model...")
model = RandomForestClassificationModel.load("/model/anomaly_detection_model_rf.spark")
print("Model loaded successfully!")

# VectorAssembler (نفس الفيتشورز بالظبط زي التدريب)
assembler = VectorAssembler(
    inputCols=[
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
    ],
    outputCol="features"
)

# متغيرات عشان نحسب الفلوز لحظيًا
flow_cache = {}  # key: (src_ip, dst_ip, src_port, dst_port, proto) → stats

def update_flow_and_predict(pkt):
    if not pkt.haslayer(IP):
        return

    ip = pkt[IP]
    src_ip = ip.src
    dst_ip = ip.dst
    proto = ip.proto  # 6=TCP, 17=UDP, 1=ICMP
    pkt_len = len(pkt)

    # جلب البورتات
    src_port = dst_port = 0
    tcp_flags = 0
    if pkt.haslayer(TCP):
        src_port = pkt[TCP].sport
        dst_port = pkt[TCP].dport
        tcp_flags = pkt[TCP].flags  # قيمة عددية زي 2=SYN, 18=SYN-ACK, إلخ
    elif pkt.haslayer(UDP):
        src_port = pkt[UDP].sport
        dst_port = pkt[UDP].dport

    flow_key = (src_ip, dst_ip, src_port, dst_port, proto)
    reverse_key = (dst_ip, src_ip, dst_port, src_port, proto)

    # نحدّث الفلو (أو نعمل واحد جديد)
    now = time.time()
    if flow_key not in flow_cache:
        flow_cache[flow_key] = {
            "start_time": now,
            "in_bytes": 0, "in_pkts": 0,
            "out_bytes": 0, "out_pkts": 0,
            "tcp_flags": 0, "client_flags": 0, "server_flags": 0,
            "ttl_list": [], "pkt_sizes": []
        }
    flow = flow_cache[flow_key]

    # تحديد الاتجاه
    if flow_key == reverse_key or flow_key in flow_cache:
        # outgoing
        flow["out_bytes"] += pkt_len
        flow["out_pkts"] += 1
        if pkt.haslayer(TCP):
            flow["server_flags"] |= tcp_flags
    else:
        # incoming
        flow["in_bytes"] += pkt_len
        flow["in_pkts"] += 1
        if pkt.haslayer(TCP):
            flow["client_flags"] |= tcp_flags

    flow["tcp_flags"] |= tcp_flags
    flow["ttl_list"].append(ip.ttl)
    flow["pkt_sizes"].append(pkt_len)

    # لو الفلو استمر أكتر من 5 ثواني → نعمل prediction ونمسح الكاش
    if now - flow["start_time"] > 5.0:
        predict_flow(flow_key, flow)
        del flow_cache[flow_key]

def predict_flow(key, flow):
    src_ip, dst_ip, src_port, dst_port, proto = key

    # حساب الفيتشورز
    duration_ms = int((time.time() - flow["start_time"]) * 1000)
    in_bytes = flow["in_bytes"]
    out_bytes = flow["out_bytes"]
    in_pkts = flow["in_pkts"]
    out_pkts = flow["out_pkts"]

    # باقي الفيتشورز بنملاها بقيم منطقية (زي اللي في التدريب)
    row = spark.range(1).drop("id") \
        .withColumn("L4_SRC_PORT", lit(src_port)) \
        .withColumn("L4_DST_PORT", lit(dst_port)) \
        .withColumn("PROTOCOL", lit(proto)) \
        .withColumn("L7_PROTO", lit(0.0 if proto not in [6,17] else 80.0)) \
        .withColumn("IN_BYTES", lit(in_bytes)) \
        .withColumn("IN_PKTS", lit(in_pkts)) \
        .withColumn("OUT_BYTES", lit(out_bytes)) \
        .withColumn("OUT_PKTS", lit(out_pkts)) \
        .withColumn("TCP_FLAGS", lit(flow["tcp_flags"])) \
        .withColumn("CLIENT_TCP_FLAGS", lit(flow["client_flags"])) \
        .withColumn("SERVER_TCP_FLAGS", lit(flow["server_flags"])) \
        .withColumn("FLOW_DURATION_MILLISECONDS", lit(duration_ms)) \
        .withColumn("DURATION_IN", lit(duration_ms // 2)) \
        .withColumn("DURATION_OUT", lit(duration_ms // 2)) \
        .withColumn("MIN_TTL", lit(min(flow["ttl_list"]) if flow["ttl_list"] else 64)) \
        .withColumn("MAX_TTL", lit(max(flow["ttl_list"]) if flow["ttl_list"] else 64)) \
        .withColumn("LONGEST_FLOW_PKT", lit(max(flow["pkt_sizes"]) if flow["pkt_sizes"] else 1500)) \
        .withColumn("SHORTEST_FLOW_PKT", lit(min(flow["pkt_sizes"]) if flow["pkt_sizes"] else 60)) \
        .withColumn("MIN_IP_PKT_LEN", lit(min(flow["pkt_sizes"]) if flow["pkt_sizes"] else 60)) \
        .withColumn("MAX_IP_PKT_LEN", lit(max(flow["pkt_sizes"]) if flow["pkt_sizes"] else 1514)) \
        .withColumn("SRC_TO_DST_SECOND_BYTES", lit(out_bytes / max((time.time() - flow["start_time"]), 0.1))) \
        .withColumn("DST_TO_SRC_SECOND_BYTES", lit(in_bytes / max((time.time() - flow["start_time"]), 0.1))) \
        .withColumn("RETRANSMITTED_IN_BYTES", lit(0)) \
        .withColumn("RETRANSMITTED_IN_PKTS", lit(0)) \
        .withColumn("RETRANSMITTED_OUT_BYTES", lit(0)) \
        .withColumn("RETRANSMITTED_OUT_PKTS", lit(0)) \
        .withColumn("SRC_TO_DST_AVG_THROUGHPUT", lit(8000000.0)) \
        .withColumn("DST_TO_SRC_AVG_THROUGHPUT", lit(8000000.0)) \
        .withColumn("NUM_PKTS_UP_TO_128_BYTES", lit(sum(1 for s in flow["pkt_sizes"] if s <= 128))) \
        .withColumn("NUM_PKTS_128_TO_256_BYTES", lit(sum(1 for s in flow["pkt_sizes"] if 128 < s <= 256))) \
        .withColumn("NUM_PKTS_256_TO_512_BYTES", lit(sum(1 for s in flow["pkt_sizes"] if 256 < s <= 512))) \
        .withColumn("NUM_PKTS_512_TO_1024_BYTES", lit(sum(1 for s in flow["pkt_sizes"] if 512 < s <= 1024))) \
        .withColumn("NUM_PKTS_1024_TO_1514_BYTES", lit(sum(1 for s in flow["pkt_sizes"] if s > 1024))) \
        .withColumn("TCP_WIN_MAX_IN", lit(65535)) \
        .withColumn("TCP_WIN_MAX_OUT", lit(65535)) \
        .withColumn("ICMP_TYPE", lit(0)) \
        .withColumn("ICMP_IPV4_TYPE", lit(0)) \
        .withColumn("DNS_QUERY_ID", lit(0)) \
        .withColumn("DNS_QUERY_TYPE", lit(0)) \
        .withColumn("DNS_TTL_ANSWER", lit(0)) \
        .withColumn("FTP_COMMAND_RET_CODE", lit(0.0))

    # Vectorize + Predict
    vec = assembler.transform(row)
    prediction = model.transform(vec)

    result = prediction.select("prediction", "probability").collect()[0]
    pred_label = "ATTACK" if result.prediction == 1 else "BENIGN"
    prob_attack = float(result.probability[1])

    print("\n" + "="*100)
    print(f" REAL-TIME ALERT | {src_ip}:{src_port} → {dst_ip}:{dst_port} | Protocol: {proto}")
    print(f" PREDICTION: {pred_label} | Attack Probability: {prob_attack:.2%}")
    if pred_label == "ATTACK":
        print(" !!! MALICIOUS TRAFFIC DETECTED! BLOCK THIS IP NOW !!!")
    print("="*100)

# ====================== بدء التقاط الباكتات ======================
print("Starting real-time packet capture... (Press Ctrl+C to stop)")
sniff(prn=update_flow_and_predict, store=False)  # يشتغل على أي واجهة

spark.stop()