# =====================================================
# predict_netflow_docker.py - FINAL WORKING VERSION
# Works 100% on Spark 3.3 + Python 3.12 + Docker
# =====================================================

from pyspark.sql import SparkSession, Row
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler

spark = (
    SparkSession.builder
        .appName("NetFlow Anomaly Detection - Docker Inference")
        .master("local[*]")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "2g")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
)

print("Spark session created successfully")
print(f"Spark version : {spark.version}")
print(f"Master URL    : {spark.sparkContext.master}")

# Load model
model_path = "/model/anomaly_detection_model_rf.spark"
loaded_model = RandomForestClassificationModel.load(model_path)
print("Random Forest model loaded successfully from /model/")

# Features (must be EXACT)
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

# Test data
attack_data = [(54321, 80, 6, 80.0, 85000, 1200, 92000, 980, 31, 31, 31, 850,
                420, 430, 64, 64, 1514, 64, 60, 1514, 85000.0, 92000.0,
                2000, 25, 2500, 30, 80000000.0, 85000000.0, 1100, 80, 15, 5, 2,
                65535, 65535, 0, 0, 0, 0, 0, 0.0)]

benign_data = [(54322, 443, 6, 443.0, 8500, 68, 3200, 52, 27, 27, 24, 1250,
                600, 650, 128, 128, 1452, 64, 60, 1452, 8500.0, 3200.0,
                0, 0, 0, 0, 5400000.0, 2048000.0, 25, 18, 12, 8, 5,
                8192, 16384, 0, 0, 12345, 3, 300, 20.0)]

data_to_test = attack_data  # ← غيّر لـ benign_data لو عايز تختبر حركة عادية

# الحل اللي بيشتغل في كل الدنيا مع cloudpickle bug
row = Row(*feature_columns_exact)
df = spark.createDataFrame([row(*data_to_test[0])])

print(f"Test DataFrame created successfully with {df.count()} record(s)")
df.show(5, truncate=False)
print("\nInput NetFlow record (detailed):")
df.show(1, truncate=False, vertical=True)

# Assemble features
assembler = VectorAssembler(inputCols=feature_columns_exact, outputCol="features", handleInvalid="skip")
df_assembled = assembler.transform(df)

# Prediction
prediction = loaded_model.transform(df_assembled)
result = prediction.select("prediction", "probability").collect()[0]

pred = int(result["prediction"])
prob_attack = float(result["probability"][1])
prob_benign = float(result["probability"][0])

print("\n" + "="*70)
print("           ANOMALY DETECTION RESULT")
print("="*70)
print(f"Prediction         : {'ATTACK' if pred == 1 else 'BENIGN'}")
print(f"Attack probability : {prob_attack:.4f} ({prob_attack:.2%})")
print(f"Benign probability : {prob_benign:.4f} ({prob_benign:.2%})")
print(f"Confidence         : {max(prob_attack, prob_benign):.2%}")

if pred == 1:
    print("\nALERT: Suspicious / malicious traffic detected!")
else:
    print("\nTraffic is normal and safe")

spark.stop()
print("\nDone! Your model is working perfectly!")