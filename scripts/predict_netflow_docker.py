# predict_netflow_docker.py - FINAL BULLETPROOF VERSION (WORKS 100%)
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder \
    .appName("NetFlow Inference - FINAL") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

print("Spark session created")

# تحميل الموديل
model = RandomForestClassificationModel.load("/model/anomaly_detection_model_rf.spark")
print("Model loaded successfully")

# نعمل صف واحد فارغ
df = spark.range(1).drop("id")


# predict_netflow_docker.py - FINAL VERSION WITH YOUR NEW TEST DATA
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder \
    .appName("NetFlow Inference - BENIGN TEST") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

print("Spark session created successfully")

# تحميل الموديل
#model = RandomForestClassificationModel.load("/models/anomaly_detection_model_rf.spark")
model = RandomForestClassificationModel.load("/models/2anomaly_detection_model_rf.spark")
print("Model loaded successfully")

# صف واحد فارغ + إضافة الأعمدة بالبيانات الجديدة (اللي تبدو طبيعية)
df = spark.range(1).drop("id")

df = df.withColumn("L4_SRC_PORT", lit(12345)) \
       .withColumn("L4_DST_PORT", lit(80)) \
       .withColumn("PROTOCOL", lit(6)) \
       .withColumn("L7_PROTO", lit(80.0)) \
       .withColumn("IN_BYTES", lit(20000)) \
       .withColumn("IN_PKTS", lit(150)) \
       .withColumn("OUT_BYTES", lit(25000)) \
       .withColumn("OUT_PKTS", lit(120)) \
       .withColumn("TCP_FLAGS", lit(31)) \
       .withColumn("CLIENT_TCP_FLAGS", lit(31)) \
       .withColumn("SERVER_TCP_FLAGS", lit(31)) \
       .withColumn("FLOW_DURATION_MILLISECONDS", lit(1000)) \
       .withColumn("DURATION_IN", lit(400)) \
       .withColumn("DURATION_OUT", lit(600)) \
       .withColumn("MIN_TTL", lit(32)) \
       .withColumn("MAX_TTL", lit(64)) \
       .withColumn("LONGEST_FLOW_PKT", lit(1500)) \
       .withColumn("SHORTEST_FLOW_PKT", lit(60)) \
       .withColumn("MIN_IP_PKT_LEN", lit(60)) \
       .withColumn("MAX_IP_PKT_LEN", lit(1514)) \
       .withColumn("SRC_TO_DST_SECOND_BYTES", lit(20000.0)) \
       .withColumn("DST_TO_SRC_SECOND_BYTES", lit(25000.0)) \
       .withColumn("RETRANSMITTED_IN_BYTES", lit(500)) \
       .withColumn("RETRANSMITTED_IN_PKTS", lit(5)) \
       .withColumn("RETRANSMITTED_OUT_BYTES", lit(600)) \
       .withColumn("RETRANSMITTED_OUT_PKTS", lit(8)) \
       .withColumn("SRC_TO_DST_AVG_THROUGHPUT", lit(2000000.0)) \
       .withColumn("DST_TO_SRC_AVG_THROUGHPUT", lit(2500000.0)) \
       .withColumn("NUM_PKTS_UP_TO_128_BYTES", lit(40)) \
       .withColumn("NUM_PKTS_128_TO_256_BYTES", lit(20)) \
       .withColumn("NUM_PKTS_256_TO_512_BYTES", lit(10)) \
       .withColumn("NUM_PKTS_512_TO_1024_BYTES", lit(5)) \
       .withColumn("NUM_PKTS_1024_TO_1514_BYTES", lit(2)) \
       .withColumn("TCP_WIN_MAX_IN", lit(8192)) \
       .withColumn("TCP_WIN_MAX_OUT", lit(16384)) \
       .withColumn("ICMP_TYPE", lit(0)) \
       .withColumn("ICMP_IPV4_TYPE", lit(0)) \
       .withColumn("DNS_QUERY_ID", lit(0)) \
       .withColumn("DNS_QUERY_TYPE", lit(0)) \
       .withColumn("DNS_TTL_ANSWER", lit(0)) \
       .withColumn("FTP_COMMAND_RET_CODE", lit(0.0))

# نضيف الأعمدة يدويًا بـ lit() ← دي الطريقة الوحيدة اللي ما بتستخدمش pickling أبدًا
"""
df = df.withColumn("L4_SRC_PORT", lit(54321)) \
       .withColumn("L4_DST_PORT", lit(80)) \
       .withColumn("PROTOCOL", lit(6)) \
       .withColumn("L7_PROTO", lit(80.0)) \
       .withColumn("IN_BYTES", lit(85000)) \
       .withColumn("IN_PKTS", lit(1200)) \
       .withColumn("OUT_BYTES", lit(92000)) \
       .withColumn("OUT_PKTS", lit(980)) \
       .withColumn("TCP_FLAGS", lit(31)) \
       .withColumn("CLIENT_TCP_FLAGS", lit(31)) \
       .withColumn("SERVER_TCP_FLAGS", lit(31)) \
       .withColumn("FLOW_DURATION_MILLISECONDS", lit(850)) \
       .withColumn("DURATION_IN", lit(420)) \
       .withColumn("DURATION_OUT", lit(430)) \
       .withColumn("MIN_TTL", lit(64)) \
       .withColumn("MAX_TTL", lit(64)) \
       .withColumn("LONGEST_FLOW_PKT", lit(1514)) \
       .withColumn("SHORTEST_FLOW_PKT", lit(64)) \
       .withColumn("MIN_IP_PKT_LEN", lit(60)) \
       .withColumn("MAX_IP_PKT_LEN", lit(1514)) \
       .withColumn("SRC_TO_DST_SECOND_BYTES", lit(85000.0)) \
       .withColumn("DST_TO_SRC_SECOND_BYTES", lit(92000.0)) \
       .withColumn("RETRANSMITTED_IN_BYTES", lit(2000)) \
       .withColumn("RETRANSMITTED_IN_PKTS", lit(25)) \
       .withColumn("RETRANSMITTED_OUT_BYTES", lit(2500)) \
       .withColumn("RETRANSMITTED_OUT_PKTS", lit(30)) \
       .withColumn("SRC_TO_DST_AVG_THROUGHPUT", lit(80000000.0)) \
       .withColumn("DST_TO_SRC_AVG_THROUGHPUT", lit(85000000.0)) \
       .withColumn("NUM_PKTS_UP_TO_128_BYTES", lit(1100)) \
       .withColumn("NUM_PKTS_128_TO_256_BYTES", lit(80)) \
       .withColumn("NUM_PKTS_256_TO_512_BYTES", lit(15)) \
       .withColumn("NUM_PKTS_512_TO_1024_BYTES", lit(5)) \
       .withColumn("NUM_PKTS_1024_TO_1514_BYTES", lit(2)) \
       .withColumn("TCP_WIN_MAX_IN", lit(65535)) \
       .withColumn("TCP_WIN_MAX_OUT", lit(65535)) \
       .withColumn("ICMP_TYPE", lit(0)) \
       .withColumn("ICMP_IPV4_TYPE", lit(0)) \
       .withColumn("DNS_QUERY_ID", lit(0)) \
       .withColumn("DNS_QUERY_TYPE", lit(0)) \
       .withColumn("DNS_TTL_ANSWER", lit(0)) \
       .withColumn("FTP_COMMAND_RET_CODE", lit(0.0))"""

print("DataFrame created successfully using lit() method (no pickling!)")

# Vector Assembler
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

df_vec = assembler.transform(df)
prediction = model.transform(df_vec)
result = prediction.select("prediction", "probability").collect()[0]

print("\n" + "="*80)
print("               NETFLOW ANOMALY DETECTION - FINAL RESULT")
print("="*80)
print(f"Prediction         : {'ATTACK' if result.prediction == 1 else 'BENIGN'}")
print(f"Attack Probability : {float(result.probability[1]):.4%}")
print(f"Confidence         : {max(float(result.probability[1]), float(result.probability[0])):.2%}")
print("ALERT: MALICIOUS TRAFFIC DETECTED! BLOCK SOURCE IMMEDIATELY!" if result.prediction == 1 else "Traffic is safe")
print("="*80)

spark.stop()