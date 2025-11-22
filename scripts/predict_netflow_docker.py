# =====================================================
# predict_netflow_docker.py - FINAL 100% WORKING VERSION
# Compatible with Pandas 2.0+ and Spark 3.3.0
# =====================================================

from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
import pandas as pd

# إضافة compatibility shim للـ iteritems (حل الـ AttributeError)
pd.DataFrame.iteritems = pd.DataFrame.items

# إنشاء Spark Session مع تعطيل Arrow optimization (يحل المشكلة الرئيسية)
spark = (
    SparkSession.builder
        .appName("NetFlow Anomaly Detection - Docker Inference")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")  # ← الحل السحري
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")  # ← تعطيل fallback
        .getOrCreate()
)

print("Spark session created successfully")
print(f"Spark version : {spark.version}")
print(f"Master URL    : {spark.sparkContext.master}")

# تحميل الموديل
model_path = "/model/anomaly_detection_model_rf.spark"
loaded_model = RandomForestClassificationModel.load(model_path)
print("Random Forest model loaded successfully from /model/")

# الأعمدة بالضبط كما في التدريب
feature_columns_exact = [
    'L4_SRC_PORT', 'L4_DST_PORT', 'PROTOCOL', 'L7_PROTO', 'IN_BYTES', 'IN_PKTS', 'OUT_BYTES',
    'OUT_PKTS', 'TCP_FLAGS', 'CLIENT_TCP_FLAGS', 'SERVER_TCP_FLAGS', 'FLOW_DURATION_MILLISECONDS',
    'DURATION_IN', 'DURATION_OUT', 'MIN_TTL', 'MAX_TTL', 'LONGEST_FLOW_PKT', 'SHORTEST_FLOW_PKT',
    'MIN_IP_PKT_LEN', 'MAX_IP_PKT_LEN', 'SRC_TO_DST_SECOND_BYTES', 'DST_TO_SRC_SECOND_BYTES',
    'RETRANSMITTED_IN_BYTES', 'RETRANSMITTED_IN_PKTS', 'RETRANSMITTED_OUT_BYTES', 'RETRANSMITTED_OUT_PKTS',
    'SRC_TO_DST_AVG_THROUGHPUT', 'DST_TO_SRC_AVG_THROUGHPUT', 'NUM_PKTS_UP_TO_128_BYTES',
    'NUM_PKTS_128_TO_256_BYTES', 'NUM_PKTS_256_TO_512_BYTES', 'NUM_PKTS_512_TO_1024_BYTES',
    'NUM_PKTS_1024_TO_1514_BYTES', 'TCP_WIN_MAX_IN', 'TCP_WIN_MAX_OUT', 'ICMP_TYPE', 'ICMP_IPV4_TYPE',
    'DNS_QUERY_ID', 'DNS_QUERY_TYPE', 'DNS_TTL_ANSWER', 'FTP_COMMAND_RET_CODE'
]

print(f"Number of features expected by the model: {len(feature_columns_exact)}")

# البيانات كـ pandas DataFrame
data = {
    'L4_SRC_PORT': [54321],
    'L4_DST_PORT': [80],
    'PROTOCOL': [6],
    'L7_PROTO': [80.0],
    'IN_BYTES': [85000],
    'IN_PKTS': [1200],
    'OUT_BYTES': [92000],
    'OUT_PKTS': [980],
    'TCP_FLAGS': [31],
    'CLIENT_TCP_FLAGS': [31],
    'SERVER_TCP_FLAGS': [31],
    'FLOW_DURATION_MILLISECONDS': [850],
    'DURATION_IN': [420],
    'DURATION_OUT': [430],
    'MIN_TTL': [64],
    'MAX_TTL': [64],
    'LONGEST_FLOW_PKT': [1514],
    'SHORTEST_FLOW_PKT': [64],
    'MIN_IP_PKT_LEN': [60],
    'MAX_IP_PKT_LEN': [1514],
    'SRC_TO_DST_SECOND_BYTES': [85000.0],
    'DST_TO_SRC_SECOND_BYTES': [92000.0],
    'RETRANSMITTED_IN_BYTES': [2000],
    'RETRANSMITTED_IN_PKTS': [25],
    'RETRANSMITTED_OUT_BYTES': [2500],
    'RETRANSMITTED_OUT_PKTS': [30],
    'SRC_TO_DST_AVG_THROUGHPUT': [80000000.0],
    'DST_TO_SRC_AVG_THROUGHPUT': [85000000.0],
    'NUM_PKTS_UP_TO_128_BYTES': [1100],
    'NUM_PKTS_128_TO_256_BYTES': [80],
    'NUM_PKTS_256_TO_512_BYTES': [15],
    'NUM_PKTS_512_TO_1024_BYTES': [5],
    'NUM_PKTS_1024_TO_1514_BYTES': [2],
    'TCP_WIN_MAX_IN': [65535],
    'TCP_WIN_MAX_OUT': [65535],
    'ICMP_TYPE': [0],
    'ICMP_IPV4_TYPE': [0],
    'DNS_QUERY_ID': [0],
    'DNS_QUERY_TYPE': [0],
    'DNS_TTL_ANSWER': [0],
    'FTP_COMMAND_RET_CODE': [0.0]
}

pdf = pd.DataFrame(data)
df = spark.createDataFrame(pdf)
print(f"Test DataFrame created successfully with {df.count()} record(s)")
df.show(1, truncate=False, vertical=True)

# Vector Assembler
assembler = VectorAssembler(
    inputCols=feature_columns_exact,
    outputCol="features",
    handleInvalid="skip"
)

df_assembled = assembler.transform(df)

# Prediction
prediction = loaded_model.transform(df_assembled)
result = prediction.select("prediction", "probability").collect()[0]

pred = int(result["prediction"])
prob_attack = float(result["probability"][1])
prob_benign = float(result["probability"][0])

print("\n" + "="*70)
print("                ANOMALY DETECTION RESULT")
print("="*70)
print(f"Prediction        : {'ATTACK' if pred == 1 else 'BENIGN'}")
print(f"Attack probability : {prob_attack:.4f} ({prob_attack:.2%})")
print(f"Benign probability : {prob_benign:.4f} ({prob_benign:.2%})")
print(f"Confidence        : {max(prob_attack, prob_benign):.2%}")

if pred == 1:
    print("ALERT: Suspicious / malicious traffic detected!")
else:
    print("Traffic is normal and safe")

spark.stop()
print("\nSpark session stopped successfully")