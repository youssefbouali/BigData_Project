# predict_netflow_docker.py - FINAL 100% WORKING VERSION
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
import pandas as pd

# إنشاء Spark Session
spark = SparkSession.builder \
    .appName("NetFlow Inference") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("Spark session created successfully")

# تحميل الموديل
model = RandomForestClassificationModel.load("/model/anomaly_detection_model_rf.spark")
print("Model loaded successfully")

# البيانات كـ pandas DataFrame (الحل السحري اللي بيشتغل دايمًا)
data = {
    "L4_SRC_PORT": [54321],
    "L4_DST_PORT": [80],
    "PROTOCOL": [6],
    "L7_PROTO": [80.0],
    "IN_BYTES": [85000],
    "IN_PKTS": [1200],
    "OUT_BYTES": [92000],
    "OUT_PKTS": [980],
    "TCP_FLAGS": [31],
    "CLIENT_TCP_FLAGS": [31],
    "SERVER_TCP_FLAGS": [31],
    "FLOW_DURATION_MILLISECONDS": [850],
    "DURATION_IN": [420],
    "DURATION_OUT": [430],
    "MIN_TTL": [64],
    "MAX_TTL": [64],
    "LONGEST_FLOW_PKT": [1514],
    "SHORTEST_FLOW_PKT": [64],
    "MIN_IP_PKT_LEN": [60],
    "MAX_IP_PKT_LEN": [1514],
    "SRC_TO_DST_SECOND_BYTES": [85000.0],
    "DST_TO_SRC_SECOND_BYTES": [92000.0],
    "RETRANSMITTED_IN_BYTES": [2000],
    "RETRANSMITTED_IN_PKTS": [25],
    "RETRANSMITTED_OUT_BYTES": [2500],
    "RETRANSMITTED_OUT_PKTS": [30],
    "SRC_TO_DST_AVG_THROUGHPUT": [80000000.0],
    "DST_TO_SRC_AVG_THROUGHPUT": [85000000.0],
    "NUM_PKTS_UP_TO_128_BYTES": [1100],
    "NUM_PKTS_128_TO_256_BYTES": [80],
    "NUM_PKTS_256_TO_512_BYTES": [15],
    "NUM_PKTS_512_TO_1024_BYTES": [5],
    "NUM_PKTS_1024_TO_1514_BYTES": [2],
    "TCP_WIN_MAX_IN": [65535],
    "TCP_WIN_MAX_OUT": [65535],
    "ICMP_TYPE": [0],
    "ICMP_IPV4_TYPE": [0],
    "DNS_QUERY_ID": [0],
    "DNS_QUERY_TYPE": [0],
    "DNS_TTL_ANSWER": [0],
    "FTP_COMMAND_RET_CODE": [0.0]
}

pdf = pd.DataFrame(data)

# تحويل من pandas إلى Spark DataFrame (يتخطى cloudpickle bug تماماً)
df = spark.createDataFrame(pdf)
print("DataFrame created successfully using pandas!")

# Vector Assembler
assembler = VectorAssembler(
    inputCols=list(data.keys()),
    outputCol="features"
)

df_vec = assembler.transform(df)
prediction = model.transform(df_vec)

# النتيجة
result = prediction.select("prediction", "probability").collect()[0]

print("\n" + "="*70)
print("           NETFLOW ANOMALY DETECTION RESULT")
print("="*70)
print(f"Prediction         : {'ATTACK' if result.prediction == 1 else 'BENIGN'}")
print(f"Attack Probability : {result.probability[1]:.4%}")
print(f"Confidence         : {max(result.probability):.2%}")
if result.prediction == 1:
    print("ALERT: HIGH-RISK TRAFFIC DETECTED! BLOCK IMMEDIATELY!")
else:
    print("Traffic is normal.")
print("="*70)

spark.stop()