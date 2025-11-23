from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from scapy.all import sniff, IP, TCP, UDP, ICMP
import time
from datetime import datetime

# تهيئة Spark Session مع إعدادات أفضل
spark = SparkSession.builder \
    .appName("RealTimeNetflowPredictor") \
    .master("local[*]") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .config("spark.cassandra.connection.host", "cassandra") \
    .config("spark.cassandra.auth.username", "cassandra") \
    .config("spark.cassandra.auth.password", "cassandra") \
    .config("spark.cassandra.output.batch.size.rows", "1") \
    .config("spark.cassandra.output.concurrent.writes", "1") \
    .getOrCreate()


#    .config("spark.sql.shuffle.partitions", "50") \

# تقليل مستوى التسجيل
spark.sparkContext.setLogLevel("ERROR")

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

# تخزين محلي للنتائج بدلاً من Cassandra مؤقتاً
results_cache = []

# إضافة هذا في بداية الملف بعد التهيئات
import csv
import os

def save_to_cassandra():
    """حفظ النتائج إلى Cassandra بشكل منفصل"""
    if not results_cache:
        return
    
    try:
        # إنشاء بيانات متوافقة مع جدول Cassandra
        data = []
        for record in results_cache:
            # تحويل IP إلى integer (طريقة بسيطة)
            src_ip_int = sum(int(x) * (256 ** i) for i, x in enumerate(reversed(record["src_ip"].split('.'))))
            dst_ip_int = sum(int(x) * (256 ** i) for i, x in enumerate(reversed(record["dst_ip"].split('.'))))
            
            data.append((
                src_ip_int,
                datetime.now(),
                dst_ip_int,
                record["src_port"],
                record["dst_port"],
                record["protocol"],
                0,  # in_bytes (قيمة افتراضية)
                0,  # out_bytes (قيمة افتراضية) 
                0,  # duration_ms (قيمة افتراضية)
                record["is_anomaly"],
                record["anomaly_score"]
            ))
        
        # إنشاء schema متوافق
        from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, TimestampType
        
        schema = StructType([
            StructField("src_ip_int", IntegerType(), True),
            StructField("ingestion_time", TimestampType(), True),
            StructField("dst_ip_int", IntegerType(), True),
            StructField("src_port", IntegerType(), True),
            StructField("dst_port", IntegerType(), True),
            StructField("protocol", IntegerType(), True),
            StructField("in_bytes", IntegerType(), True),
            StructField("out_bytes", IntegerType(), True),
            StructField("duration_ms", IntegerType(), True),
            StructField("is_anomaly", IntegerType(), True),
            StructField("anomaly_score", DoubleType(), True)
        ])
        
        df = spark.createDataFrame(data, schema)
        
        # محاولة الحفظ إلى Cassandra
        df.write \
            .format("org.apache.spark.sql.cassandra") \
            .option("table", "predictions") \
            .option("keyspace", "netflow") \
            .mode("append") \
            .save()
        
        print(f"✓ Saved {len(results_cache)} records to Cassandra")
        results_cache.clear()
        
    except Exception as e:
        print(f"✗ Failed to save to Cassandra: {str(e)[:200]}")





from pyspark.sql.functions import col, trim, to_timestamp

def csv_to_cassandra(name):
    CSV_PATH = "/data/predictions_"+name+".csv"

    df = spark.read.option("header", "true") \
                   .option("inferSchema", "false") \
                   .csv(CSV_PATH)

    # ===================================================================
    # Nettoyage léger + conversion types
    # ===================================================================
    df_clean = df \
        .withColumn("src_ip", trim(col("src_ip"))) \
        .withColumn("dst_ip", trim(col("dst_ip"))) \
        .withColumn("src_port", col("src_port").cast("int")) \
        .withColumn("dst_port", col("dst_port").cast("int")) \
        .withColumn("protocol", col("protocol").cast("int")) \
        .withColumn("is_anomaly", col("is_anomaly").cast("int")) \
        .withColumn("anomaly_score", col("anomaly_score").cast("double")) \
        .withColumn("ingestion_time", to_timestamp(col("timestamp")))  # تحويل ISO8601 → TIMESTAMP

    # حذف العمود القديم
    df_clean = df_clean.drop("timestamp")


    df_clean.write \
        .format("org.apache.spark.sql.cassandra") \
        .options(table="predictions", keyspace="netflow") \
        .mode("append") \
        .save()
    os.remove("/data/predictions_"+name+".csv")




import uuid


def save_to_csv():
    """حفظ النتائج إلى ملف CSV مؤقتاً"""
    if not results_cache:
        return
    
    try:
        random_name = uuid.uuid4().hex
        file_exists = os.path.isfile(f"/data/predictions_{random_name}.csv")
        with open(f"/data/predictions_{random_name}.csv", 'a', newline='') as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(['src_ip', 'dst_ip', 'src_port', 'dst_port', 'protocol', 'is_anomaly', 'anomaly_score', 'timestamp'])
            
            for record in results_cache:
                writer.writerow([
                    record["src_ip"],
                    record["dst_ip"],
                    record["src_port"],
                    record["dst_port"],
                    record["protocol"],
                    record["is_anomaly"],
                    record["anomaly_score"],
                    record["timestamp"]
                    #datetime.now().isoformat()
                ])
        
        
        from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

        schema = StructType([
            StructField("src_ip", StringType(), True),
            StructField("dst_ip", StringType(), True),
            StructField("src_port", IntegerType(), True),
            StructField("dst_port", IntegerType(), True),
            StructField("protocol", StringType(), True),
            StructField("is_anomaly", IntegerType(), True),
            StructField("anomaly_score", FloatType(), True),
            StructField("timestamp", StringType(), True)
        ])

        df = spark.createDataFrame(results_cache, schema)

        
        df_clean = df \
            .withColumn("src_ip", trim(col("src_ip"))) \
            .withColumn("dst_ip", trim(col("dst_ip"))) \
            .withColumn("src_port", col("src_port").cast("int")) \
            .withColumn("dst_port", col("dst_port").cast("int")) \
            .withColumn("protocol", col("protocol").cast("int")) \
            .withColumn("is_anomaly", col("is_anomaly").cast("int")) \
            .withColumn("anomaly_score", col("anomaly_score").cast("double")) \
            .withColumn("ingestion_time", to_timestamp(col("timestamp")))  # تحويل ISO8601 → TIMESTAMP

        # حذف العمود القديم
        df_clean = df_clean.drop("timestamp")


        df_clean.write \
            .format("org.apache.spark.sql.cassandra") \
            .options(table="predictions", keyspace="netflow") \
            .mode("append") \
            .save()
        
        
        
        
        #csv_to_cassandra(random_name)
        print(f"✓ Saved {len(results_cache)} records to CSV")
        results_cache.clear()
    except Exception as e:
        print(f"✗ Failed to save to CSV: {e}")











# ثم استبدل save_to_cassandra() بـ save_to_csv() في الكود

def predict_packet(pkt):
    """معالجة حزمة شبكة واحدة"""
    if not pkt.haslayer(IP):
        return

    try:
        ip = pkt[IP]
        src_ip = ip.src
        dst_ip = ip.dst
        proto = ip.proto
        pkt_len = len(pkt)
        ttl = ip.ttl

        # استخراج المنافذ
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

        # إنشاء DataFrame ببيانات افتراضية
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

        # التنبؤ بالنموذج
        vec = assembler.transform(df)
        result = model.transform(vec).select("prediction", "probability").collect()[0]
        
        label = "ATTACK" if result.prediction == 1 else "BENIGN"
        prob = float(result.probability[1]) * 100

        # تخزين النتيجة
        record = {
            "src_ip": src_ip,
            "dst_ip": dst_ip,
            "src_port": src_port,
            "dst_port": dst_port,
            "protocol": proto,
            "is_anomaly": int(result.prediction),
            "anomaly_score": prob,
            "timestamp": datetime.now().isoformat()
        }
        
        results_cache.append(record)

        # عرض النتيجة
        print("\n" + "="*80)
        print(f" REAL-TIME DETECTION | {src_ip}:{src_port} → {dst_ip}:{dst_port} (Proto: {proto})")
        print(f" PREDICTION → {label} | Attack Probability: {prob:.2f}%")
        if label == "ATTACK":
            print(" ⚠️  MALICIOUS TRAFFIC DETECTED! BLOCK THIS FLOW NOW!")
        print("="*80)

        # حفظ إلى Cassandra كل 5 نتائج
        if len(results_cache) >= 5:
            save_to_csv()

    except Exception as e:
        print(f"❌ Prediction error: {str(e)[:100]}")

# بدء المراقبة
print("🚀 Starting real-time detection (every packet triggers prediction)...")
print("📡 Try: curl google.com OR hping3 --flood inside/outside the container")
print("💾 Results will be saved to Cassandra every 5 packets")

try:
    sniff(prn=predict_packet, store=False, iface="eth0")
except KeyboardInterrupt:
    print("\n🛑 Stopping real-time detection...")
    # حفظ أي نتائج متبقية
    if results_cache:
        save_to_csv()
finally:
    spark.stop()
    print("✅ Spark session closed.")