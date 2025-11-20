from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.ml import PipelineModel

# -------------------------------
# 1. Session Spark
# -------------------------------
spark = SparkSession.builder \
    .appName("Test-Single-Input") \
    .master("local[*]") \
    .getOrCreate()

# -------------------------------
# 2. Charger le modèle
# -------------------------------
model_path = "2anomaly_detection_model_rf.spark"
model = PipelineModel.load(model_path)
print("Modèle chargé avec succès !")

# -------------------------------
# 3. Définir le schéma des features
# -------------------------------
flow_schema = StructType([
    StructField("src_ip", StringType(), True),
    StructField("dst_ip", StringType(), True),
    StructField("src_ip_int", IntegerType(), True),
    StructField("dst_ip_int", IntegerType(), True),
    StructField("src_port", IntegerType(), True),
    StructField("dst_port", IntegerType(), True),
    StructField("protocol", IntegerType(), True),
    StructField("l7_proto", DoubleType(), True),
    StructField("in_bytes", LongType(), True),
    StructField("out_bytes", LongType(), True),
    StructField("in_pkts", LongType(), True),
    StructField("out_pkts", LongType(), True),
    StructField("duration_ms", LongType(), True),
    StructField("tcp_flags", IntegerType(), True),
    StructField("client_tcp_flags", IntegerType(), True),
    StructField("server_tcp_flags", IntegerType(), True),
    StructField("src_to_dst_avg_throughput", DoubleType(), True),
    StructField("dst_to_src_avg_throughput", DoubleType(), True),
    StructField("num_pkts_up_to_128_bytes", LongType(), True),
    StructField("num_pkts_128_to_256_bytes", LongType(), True),
    StructField("num_pkts_256_to_512_bytes", LongType(), True),
    StructField("num_pkts_512_to_1024_bytes", LongType(), True),
    StructField("num_pkts_1024_to_1514_bytes", LongType(), True)
])

# -------------------------------
# 4. Créer un DataFrame avec un seul exemple
# -------------------------------
sample_data = [
    ( "192.168.0.1", "10.0.0.1", 3232235521, 167772161, 12345, 80, 6, 0.0,
      5000, 3000, 50, 48, 1000, 24, 24, 0, 100.0, 60.0, 20, 15, 10, 8, 2 )
]

df = spark.createDataFrame(sample_data, schema=flow_schema)

# -------------------------------
# 5. Faire la prédiction
# -------------------------------
predictions = model.transform(df)
predictions.select("prediction", "probability").show(truncate=False)

# -------------------------------
# 6. Fin
# -------------------------------
spark.stop()
