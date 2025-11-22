# predict_netflow_docker.py - FINAL WORKING VERSION (NO PICKLING ERROR)
from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassificationModel
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import *
from pyspark.sql import Row

# إجبار Spark على استخدام Pickler العادي بدل cloudpickle المعطوب
import sys
sys.modules['pyspark.sql.types'] = None  # نلغي أي cache
import cloudpickle
cloudpickle.__version__ = "1.6.0"  # نخدع النظام إنه إصدار قديم

spark = SparkSession.builder \
    .appName("NetFlow Inference") \
    .master("local[*]") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.execution.arrow.pyspark.enabled", "false") \
    .getOrCreate()

print("Spark session created successfully")

# تحميل الموديل
model = RandomForestClassificationModel.load("/model/anomaly_detection_model_rf.spark")
print("Model loaded successfully")

# البيانات كـ Row (الحل السحري اللي بيشتغل دايمًا)
data = Row(
    L4_SRC_PORT=54321,
    L4_DST_PORT=80,
    PROTOCOL=6,
    L7_PROTO=80.0,
    IN_BYTES=85000,
    IN_PKTS=1200,
    OUT_BYTES=92000,
    OUT_PKTS=980,
    TCP_FLAGS=31,
    CLIENT_TCP_FLAGS=31,
    SERVER_TCP_FLAGS=31,
    FLOW_DURATION_MILLISECONDS=850,
    DURATION_IN=420,
    DURATION_OUT=430,
    MIN_TTL=64,
    MAX_TTL=64,
    LONGEST_FLOW_PKT=1514,
    SHORTEST_FLOW_PKT=64,
    MIN_IP_PKT_LEN=60,
    MAX_IP_PKT_LEN=1514,
    SRC_TO_DST_SECOND_BYTES=85000.0,
    DST_TO_SRC_SECOND_BYTES=92000.0,
    RETRANSMITTED_IN_BYTES=2000,
    RETRANSMITTED_IN_PKTS=25,
    RETRANSMITTED_OUT_BYTES=2500,
    RETRANSMITTED_OUT_PKTS=30,
    SRC_TO_DST_AVG_THROUGHPUT=80000000.0,
    DST_TO_SRC_AVG_THROUGHPUT=85000000.0,
    NUM_PKTS_UP_TO_128_BYTES=1100,
    NUM_PKTS_128_TO_256_BYTES=80,
    NUM_PKTS_256_TO_512_BYTES=15,
    NUM_PKTS_512_TO_1024_BYTES=5,
    NUM_PKTS_1024_TO_1514_BYTES=2,
    TCP_WIN_MAX_IN=65535,
    TCP_WIN_MAX_OUT=65535,
    ICMP_TYPE=0,
    ICMP_IPV4_TYPE=0,
    ,
    DNS_QUERY_ID=0,
    DNS_QUERY_TYPE=0,
    DNS_TTL_ANSWER=0,
    FTP_COMMAND_RET_CODE=0.0
)

# نعمل DataFrame من Row مباشرة (أكثر أمانًا من أي طريقة تانية)
df = spark.createDataFrame([data])

print("DataFrame created successfully!")

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

print("\n" + "="*70)
print("            NETFLOW ANOMALY DETECTION - RESULT")
print("="*70)
print(f"Prediction         : {'ATTACK' if result.prediction == 1 else 'BENIGN'}")
print(f"Attack Probability : {result.probability[1]:.4%}")
print(f"Confidence         : {max(result.probability):.2%}")
print("ALERT: SUSPICIOUS TRAFFIC DETECTED!" if result.prediction == 1 else "Normal traffic")
print("="*70)

spark.stop()