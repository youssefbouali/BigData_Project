# predict_netflow_docker.py - FINAL WORKING 100%
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler

spark = SparkSession.builder \
    .appName("NetFlow Inference") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("Spark session created successfully")
print(f"Spark version: {spark.version}")

# تحميل الموديل
model_path = "/model/anomaly_detection_model_rf.spark"
model = RandomForestClassificationModel.load(model_path)
print("Model loaded successfully")

# Schema بالترتيب الصحيح (41 عمود)
schema = StructType([
    StructField("L4_SRC_PORT", IntegerType(), True),
    StructField("L4_DST_PORT", IntegerType(), True),
    StructField("PROTOCOL", IntegerType(), True),
    StructField("L7_PROTO", DoubleType(), True),
    StructField("IN_BYTES", LongType(), True),
    StructField("IN_PKTS", LongType(), True),
    StructField("OUT_BYTES", LongType(), True),
    StructField("OUT_PKTS", LongType(), True),
    StructField("TCP_FLAGS", IntegerType(), True),
    StructField("CLIENT_TCP_FLAGS", IntegerType(), True),
    StructField("SERVER_TCP_FLAGS", IntegerType(), True),
    StructField("FLOW_DURATION_MILLISECONDS", LongType(), True),
    StructField("DURATION_IN", LongType(), True),
    StructField("DURATION_OUT", LongType(), True),
    StructField("MIN_TTL", IntegerType(), True),
    StructField("MAX_TTL", IntegerType(), True),
    StructField("LONGEST_FLOW_PKT", LongType(), True),
    StructField("SHORTEST_FLOW_PKT", IntegerType(), True),
    StructField("MIN_IP_PKT_LEN", IntegerType(), True),
    StructField("MAX_IP_PKT_LEN", LongType(), True),
    StructField("SRC_TO_DST_SECOND_BYTES", DoubleType(), True),
    StructField("DST_TO_SRC_SECOND_BYTES", DoubleType(), True),
    StructField("RETRANSMITTED_IN_BYTES", LongType(), True),
    StructField("RETRANSMITTED_IN_PKTS", LongType(), True),
    StructField("RETRANSMITTED_OUT_BYTES", LongType(), True),
    StructField("RETRANSMITTED_OUT_PKTS", LongType(), True),
    StructField("SRC_TO_DST_AVG_THROUGHPUT", DoubleType(), True),
    StructField("DST_TO_SRC_AVG_THROUGHPUT", DoubleType(), True),
    StructField("NUM_PKTS_UP_TO_128_BYTES", LongType(), True),
    StructField("NUM_PKTS_128_TO_256_BYTES", LongType(), True),
    StructField("NUM_PKTS_256_TO_512_BYTES", LongType(), True),
    StructField("NUM_PKTS_512_TO_1024_BYTES", LongType(), True),
    StructField("NUM_PKTS_1024_TO_1514_BYTES", LongType(), True),
    StructField("TCP_WIN_MAX_IN", LongType(), True),
    StructField("TCP_WIN_MAX_OUT", LongType(), True),
    StructField("ICMP_TYPE", IntegerType(), True),
    StructField("ICMP_IPV4_TYPE", IntegerType(), True),
    StructField("DNS_QUERY_ID", IntegerType(), True),
    StructField("DNS_QUERY_TYPE", IntegerType(), True),
    StructField("DNS_TTL_ANSWER", LongType(), True),
    StructField("FTP_COMMAND_RET_CODE", DoubleType(), True)
])

# البيانات كـ list عادية (مش tuple ولا Row)
data = [
    54321, 80, 6, 80.0, 85000, 1200, 92000, 980, 31, 31, 31, 850,
    420, 430, 64, 64, 1514, 64, 60, 1514, 85000.0, 92000.0,
    2000, 25, 2500, 30, 80000000.0, 85000000.0, 1100, 80, 15, 5, 2,
    65535, 65535, 0, 0, 0, 0, 0, 0.0
]

# إنشاء DataFrame بأمان تام
df = spark.createDataFrame([data], schema)
print("DataFrame created successfully!")

# Vector Assembly + Prediction
assembler = VectorAssembler(inputCols=schema.names, outputCol="features")
df_vec = assembler.transform(df)
prediction = model.transform(df_vec)

result = prediction.select("prediction", "probability").collect()[0]
pred_label = "ATTACK" if result.prediction == 1 else "BENIGN"
attack_prob = result.probability[1]

print("\n" + "="*60)
print("           NETFLOW ANOMALY DETECTION RESULT")
print("="*60)
print(f"Prediction          : {pred_label}")
print(f"Attack Probability  : {attack_prob:.4%}")
print(f"Confidence          : {max(result.probability):.2%}")
print("ALERT: HIGH-RISK TRAFFIC DETECTED!" if result.prediction == 1 else "Traffic appears normal")
print("="*60)

spark.stop()